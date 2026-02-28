#!/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# migrate_influx2_to_influx3.py - Migrate SensorPush data from InfluxDB 2 to InfluxDB 3
# -----------------------------------------------------------------------------
#
# This tool migrates SensorPush data from InfluxDB 2.x to InfluxDB 3.x.
# Based on migrate_influx2vm.py.
#
# Optimizations:
#   - Streaming reads via query_stream() — records are processed one at a time
#     instead of loading entire query results into memory
#   - Batched writes via WriteType.batching — the InfluxDB 3 client automatically
#     buffers records and flushes in efficient HTTP batches
#

import sys
import time
import json
import signal
import argparse
import configparser
import urllib3
import requests as req
from pathlib import Path
from datetime import datetime, timedelta
from os.path import expanduser

# Disable SSL warnings
urllib3.disable_warnings()

default_ifdb2_config = expanduser("~/.sensorpush.conf")
default_ifdb3_config = expanduser("~/.sensorpushd.conf")

parser = argparse.ArgumentParser(
    description='Migrate SensorPush data from InfluxDB 2.x to InfluxDB 3.x',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  %(prog)s --all --dry-run                    # Dry-run migration of all data
  %(prog)s --all                              # Migrate all data
  %(prog)s --all --resume                     # Resume an interrupted migration
  %(prog)s --start 2024-01-01 --end 2024-12-31
  %(prog)s --start -30d --dry-run
  %(prog)s --ifdb2-config /path/to/ifdb2.conf --ifdb3-config /path/to/ifdb3.conf --all
"""
)

parser.add_argument('--ifdb2-config', dest='ifdb2_config', default=default_ifdb2_config,
                    help=f'Path to InfluxDB 2 config file (default: {default_ifdb2_config})')
parser.add_argument('--ifdb3-config', dest='ifdb3_config', default=default_ifdb3_config,
                    help=f'Path to InfluxDB 3 config file (default: {default_ifdb3_config})')
parser.add_argument('--start', dest='start_time', default=None,
                    help='Start time for migration (e.g., 2024-01-01, -30d, -1y)')
parser.add_argument('--end', dest='end_time', default='now',
                    help='End time for migration (e.g., 2024-12-31, now) (default: now)')
parser.add_argument('--all', dest='migrate_all', action='store_true', default=False,
                    help='Migrate all data from InfluxDB 2 (from earliest available)')
parser.add_argument('--batch-size', dest='batch_size', type=int, default=5000,
                    help='Number of records per batch write (default: 5000)')
parser.add_argument('--chunk-days', dest='chunk_days', type=int, default=7,
                    help='Number of days per query chunk when using --all (default: 7)')
parser.add_argument('--dry-run', '-t', dest='dryrun', action='store_true', default=False,
                    help='Do not write any data - just show what would be migrated')
parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', default=False,
                    help='Enable verbose output')
parser.add_argument('--resume', dest='resume', action='store_true', default=False,
                    help='Resume a previously interrupted migration from where it left off')
parser.add_argument('--progress-file', dest='progress_file',
                    default=expanduser('~/.migrate_ifdb2_to_ifdb3_progress.json'),
                    help='Path to progress file for resume support (default: ~/.migrate_ifdb2_to_ifdb3_progress.json)')

args = parser.parse_args()

# Validate arguments
if not args.start_time and not args.migrate_all:
    print('Error: Either --start or --all must be specified', file=sys.stderr)
    parser.print_help()
    sys.exit(1)

if args.migrate_all:
    args.start_time = '1970-01-01'

# ---------------------------------------------------------------------------
# Read InfluxDB 2 (source) configuration
# ---------------------------------------------------------------------------
if not Path(args.ifdb2_config).is_file():
    print(f'Error: InfluxDB 2 config file not found: {args.ifdb2_config}', file=sys.stderr)
    sys.exit(1)

ifdb2_config = configparser.ConfigParser()
ifdb2_config.read(args.ifdb2_config)

# Support both legacy ([INFLUXDBCONF]) and unified ([INFLUXDB2]) section names
if 'INFLUXDBCONF' in ifdb2_config:
    sec = 'INFLUXDBCONF'
    IFDB2_URL = ifdb2_config[sec]['IFDB_URL']
    IFDB2_PORT = ifdb2_config[sec]['IFDB_PORT']
    IFDB2_ORG = ifdb2_config[sec]['IFDB_ORG']
    IFDB2_BUCKET = ifdb2_config[sec]['IFDB_BUCKET']
    IFDB2_TOKEN = ifdb2_config[sec]['IFDB_TOKEN']
    IFDB2_VERIFY_SSL = ifdb2_config[sec].get('IFDB_VERIFY_SSL', 'True').lower() in ['true', '1']
    MEASUREMENT_NAME = ifdb2_config[sec]['MEASUREMENT_NAME']
elif 'INFLUXDB2' in ifdb2_config:
    sec = 'INFLUXDB2'
    url = ifdb2_config[sec].get('URL', 'http://localhost:8086')
    if ifdb2_config.has_option(sec, 'PORT'):
        port = ifdb2_config[sec]['PORT']
        host_part = url.split('://', 1)[-1]
        if ':' not in host_part:
            url = f'{url}:{port}'
    IFDB2_URL = url.rsplit(':', 1)[0] if '://' in url and url.count(':') > 1 else url
    IFDB2_PORT = url.rsplit(':', 1)[1] if '://' in url and url.count(':') > 1 else '8086'
    IFDB2_ORG = ifdb2_config[sec].get('ORG', '')
    IFDB2_BUCKET = ifdb2_config[sec].get('BUCKET', 'sensorpush')
    IFDB2_TOKEN = ifdb2_config[sec].get('TOKEN', '')
    IFDB2_VERIFY_SSL = ifdb2_config[sec].get('VERIFY_SSL', 'False').lower() in ['true', '1']
    MEASUREMENT_NAME = ifdb2_config[sec].get('MEASUREMENT_NAME', 'SensorPush')
else:
    print('Error: No [INFLUXDBCONF] or [INFLUXDB2] section found in source config', file=sys.stderr)
    sys.exit(1)

# Build the full InfluxDB 2 URL
IFDB2_FULL_URL = IFDB2_URL
host_part = IFDB2_FULL_URL.split('://', 1)[-1]
if ':' not in host_part:
    IFDB2_FULL_URL = f'{IFDB2_FULL_URL}:{IFDB2_PORT}'

# ---------------------------------------------------------------------------
# Read InfluxDB 3 (destination) configuration
# ---------------------------------------------------------------------------
if not Path(args.ifdb3_config).is_file():
    print(f'Error: InfluxDB 3 config file not found: {args.ifdb3_config}', file=sys.stderr)
    sys.exit(1)

ifdb3_config = configparser.ConfigParser()
ifdb3_config.read(args.ifdb3_config)

if 'INFLUXDB3' not in ifdb3_config:
    print('Error: No [INFLUXDB3] section found in destination config', file=sys.stderr)
    sys.exit(1)

IFDB3_URL = ifdb3_config['INFLUXDB3'].get('URL',
            ifdb3_config['INFLUXDB3'].get('HOST', 'http://localhost:8181'))
IFDB3_DATABASE = ifdb3_config['INFLUXDB3'].get('DATABASE', 'sensorpush')
IFDB3_TOKEN = ifdb3_config['INFLUXDB3'].get('TOKEN', '')
IFDB3_VERIFY_SSL = ifdb3_config['INFLUXDB3'].get('VERIFY_SSL', 'False').lower() in ['true', '1']
IFDB3_MEASUREMENT_NAME = ifdb3_config['INFLUXDB3'].get('MEASUREMENT_NAME', 'SensorPush')


def parse_time_arg(time_arg):
    """Parse time argument and return Flux-compatible time string."""
    if time_arg == 'now':
        return 'now()'
    if time_arg.startswith('-') and time_arg[-1] in 'dhms':
        return time_arg
    try:
        dt = datetime.fromisoformat(time_arg)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        print(f'Error: Invalid time format: {time_arg}', file=sys.stderr)
        print('Use ISO format (YYYY-MM-DD) or relative format (-30d, -1y)', file=sys.stderr)
        sys.exit(1)


def generate_time_chunks(start_dt, end_dt, chunk_days):
    """Generate time chunks for querying InfluxDB."""
    chunks = []
    current = start_dt
    delta = timedelta(days=chunk_days)
    while current < end_dt:
        chunk_end = min(current + delta, end_dt)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks


def load_progress():
    """Load progress from the progress file, if it exists."""
    pf = Path(args.progress_file)
    if pf.is_file():
        try:
            with open(pf) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f'Warning: Could not read progress file: {e}', file=sys.stderr)
    return None


def save_progress(completed_chunk_end, chunk_index, total_chunks,
                  records_read, records_written):
    """Save progress after a successfully completed chunk."""
    progress = {
        'completed_through': completed_chunk_end,
        'chunk_index': chunk_index,
        'total_chunks': total_chunks,
        'records_read': records_read,
        'records_written': records_written,
        'timestamp': datetime.now().isoformat(),
        'bucket': IFDB2_BUCKET,
        'measurement': MEASUREMENT_NAME,
    }
    try:
        with open(args.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    except OSError as e:
        print(f'Warning: Could not save progress file: {e}', file=sys.stderr)


def remove_progress():
    """Remove the progress file after successful completion."""
    pf = Path(args.progress_file)
    if pf.is_file():
        try:
            pf.unlink()
        except OSError:
            pass


# Graceful Ctrl+C handling — save progress before exiting
interrupted = False


def sigint_handler(signum, frame):
    global interrupted
    if interrupted:
        print('\nForce quit.', file=sys.stderr)
        sys.exit(1)
    interrupted = True
    print('\nInterrupted — finishing current chunk before saving progress...',
          file=sys.stderr)


signal.signal(signal.SIGINT, sigint_handler)


start_time = parse_time_arg(args.start_time)
end_time = parse_time_arg(args.end_time)

print(f'Migration settings:')
print(f'  Source InfluxDB 2: {IFDB2_FULL_URL}, bucket: {IFDB2_BUCKET}')
print(f'  Dest   InfluxDB 3: {IFDB3_URL}, database: {IFDB3_DATABASE}')
print(f'  Source measurement: {MEASUREMENT_NAME}')
print(f'  Target measurement: {IFDB3_MEASUREMENT_NAME}')
print(f'  Time range: {start_time} to {end_time}')
print(f'  Batch size: {args.batch_size}')
print(f'  Dry run: {args.dryrun}')
print()

# ---------------------------------------------------------------------------
# Connect to InfluxDB 2 (source) — streaming query API
# ---------------------------------------------------------------------------
try:
    from influxdb_client import InfluxDBClient
except ImportError:
    print("Error: 'influxdb-client' package required. Install with: pip install influxdb-client",
          file=sys.stderr)
    sys.exit(1)

print('Connecting to InfluxDB 2 (source)...')
ifdbc2 = InfluxDBClient(
    url=IFDB2_FULL_URL,
    token=IFDB2_TOKEN,
    org=IFDB2_ORG,
    verify_ssl=IFDB2_VERIFY_SSL,
    timeout=(60000, 600000)   # 60s connect, 600s read
)
ifdbc2_query = ifdbc2.query_api()

# ---------------------------------------------------------------------------
# Connect to InfluxDB 3 (destination) — batching write API
# ---------------------------------------------------------------------------
write_errors = []

if not args.dryrun:
    try:
        from influxdb_client_3 import (InfluxDBClient3, write_client_options,
                                        WriteOptions, WritePrecision)
        from influxdb_client_3.write_client.client.write_api import WriteType
    except ImportError:
        print("Error: 'influxdb3-python' package required. Install with: pip install influxdb3-python",
              file=sys.stderr)
        sys.exit(1)

    def _on_write_success(conf, data):
        if args.verbose:
            lines = data.split('\n') if isinstance(data, str) else [data]
            print(f'    Batch flushed: {len(lines)} lines')

    def _on_write_error(conf, data, exception):
        write_errors.append(str(exception))
        print(f'  ERROR writing batch: {exception}', file=sys.stderr)

    def _on_write_retry(conf, data, exception):
        if args.verbose:
            print(f'    Retrying batch: {exception}')

    print('Connecting to InfluxDB 3 (destination) [batching mode]...')
    wco = write_client_options(
        write_options=WriteOptions(
            write_type=WriteType.batching,
            batch_size=args.batch_size,
            flush_interval=10_000,      # flush at least every 10 seconds
            retry_interval=5_000,
            max_retries=5,
            max_retry_delay=30_000,
            exponential_base=2,
            max_close_wait=300_000,     # wait up to 5 min for flush on close
        ),
        success_callback=_on_write_success,
        error_callback=_on_write_error,
        retry_callback=_on_write_retry,
    )
    ifdbc3 = InfluxDBClient3(
        host=IFDB3_URL,
        database=IFDB3_DATABASE,
        token=IFDB3_TOKEN,
        write_client_options=wco
    )
    print(f'  Connected to InfluxDB 3 at {IFDB3_URL}')
else:
    ifdbc3 = None

print()

# ---------------------------------------------------------------------------
# Processing — streaming reads, batched writes
# ---------------------------------------------------------------------------

# InfluxDB 2 metadata keys to skip (not measurement fields)
excluded_keys = {'_start', '_stop', '_time', '_measurement', 'result', 'table',
                 'sensor_id', 'sensor_name', '_field', '_value'}

total_records = 0
total_written = 0
batches_written = 0
sample_record = None


def process_stream(record_stream):
    """Process a streaming iterator of FluxRecords, writing to InfluxDB 3.

    Uses query_stream() on the read side (one record at a time, no full
    result set in memory) and the batching WriteOptions on the write side
    (client auto-flushes every batch_size records or flush_interval ms).
    """
    global total_records, total_written, batches_written, sample_record

    batch = []

    for record in record_stream:
        total_records += 1

        ts = record.get_time()
        rec_measurement = record.values.get('_measurement', MEASUREMENT_NAME)

        target_measurement = IFDB3_MEASUREMENT_NAME
        if rec_measurement.endswith('_V'):
            target_measurement = f'{IFDB3_MEASUREMENT_NAME}_V'

        tags = {}
        if 'sensor_id' in record.values and record.values['sensor_id'] is not None:
            tags['sensor_id'] = str(record.values['sensor_id'])
        if 'sensor_name' in record.values and record.values['sensor_name'] is not None:
            tags['sensor_name'] = str(record.values['sensor_name'])

        fields = {}
        for field_name, field_value in record.values.items():
            if field_name in excluded_keys or field_value is None:
                continue
            try:
                fields[field_name] = float(field_value)
            except (ValueError, TypeError):
                pass

        if not fields:
            continue

        rec = {
            'measurement': target_measurement,
            'tags': tags,
            'fields': fields,
            'time': ts.isoformat(),
        }

        if sample_record is None:
            sample_record = rec

        if args.dryrun:
            total_written += 1
        else:
            batch.append(rec)

            # Feed records to the batching writer in chunks to avoid
            # building one giant list in memory
            if len(batch) >= args.batch_size:
                ifdbc3.write(record=batch)
                total_written += len(batch)
                batches_written += 1
                batch = []

                if total_written % (args.batch_size * 10) == 0:
                    print(f'    Progress: {total_written:,} records written...')

    # Write any remaining records
    if batch and not args.dryrun:
        ifdbc3.write(record=batch)
        total_written += len(batch)
        batches_written += 1


def build_flux_query(bucket, measurement, start, stop):
    """Build a Flux query with pivot for the given time range."""
    return f'''from(bucket: "{bucket}")
  |> range(start: {start}, stop: {stop})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}" or r["_measurement"] == "{measurement}_V")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''


# ---------------------------------------------------------------------------
# Run migration
# ---------------------------------------------------------------------------
use_chunking = args.migrate_all or (
    args.start_time and args.start_time.startswith('-') and
    args.start_time[-1] == 'd' and int(args.start_time[1:-1]) > 30
)

migration_start = time.time()

if use_chunking and args.migrate_all:
    print('Finding earliest data in InfluxDB 2...')

    ifdb_database = IFDB2_BUCKET.split('/')[0] if '/' in IFDB2_BUCKET else IFDB2_BUCKET
    influxql_query = f'SELECT * FROM "{MEASUREMENT_NAME}" ORDER BY time ASC LIMIT 1'
    influxql_url = f'{IFDB2_FULL_URL}/query'

    if args.verbose:
        print(f'  Query: {influxql_query}')

    try:
        response = req.get(
            influxql_url,
            params={'db': ifdb_database, 'q': influxql_query},
            headers={'Authorization': f'Token {IFDB2_TOKEN}'},
            verify=IFDB2_VERIFY_SSL,
            timeout=60
        )

        if response.status_code != 200:
            print(f'Error querying InfluxDB 2: {response.status_code} {response.text}', file=sys.stderr)
            sys.exit(1)

        data = response.json()
        if 'results' in data and data['results'] and 'series' in data['results'][0]:
            first_time_str = data['results'][0]['series'][0]['values'][0][0]
            first_time = datetime.fromisoformat(first_time_str.replace('Z', '+00:00'))
            print(f'  Earliest data found: {first_time}')
            start_dt = first_time.replace(tzinfo=None)
        else:
            print('  No data found in InfluxDB 2')
            sys.exit(0)
    except Exception as e:
        print(f'Error finding earliest data: {e}', file=sys.stderr)
        sys.exit(1)

    end_dt = datetime.now()
    chunks = generate_time_chunks(start_dt, end_dt, args.chunk_days)
    print(f'  Will process {len(chunks)} time chunks of {args.chunk_days} days each')

    # Resume support: skip already-completed chunks
    skip_until = None
    if args.resume:
        progress = load_progress()
        if progress:
            if progress.get('bucket') != IFDB2_BUCKET or \
               progress.get('measurement') != MEASUREMENT_NAME:
                print(f'  Warning: Progress file is for a different bucket/measurement '
                      f'({progress.get("bucket")}/{progress.get("measurement")}), ignoring.',
                      file=sys.stderr)
            else:
                skip_until = progress['completed_through']
                print(f'  Resuming after: {skip_until}')
                print(f'  (Previously: {progress["records_read"]:,} read, '
                      f'{progress["records_written"]:,} written, '
                      f'chunk {progress["chunk_index"]+1}/{progress["total_chunks"]})')
                total_records = progress['records_read']
                total_written = progress['records_written']
        else:
            print('  No progress file found, starting from the beginning.')

    print()

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        chunk_start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Skip already-completed chunks when resuming
        if skip_until and chunk_end_str <= skip_until:
            if args.verbose:
                print(f'Chunk {i+1}/{len(chunks)}: {chunk_start_str} to {chunk_end_str} — skipped (already done)')
            continue

        if interrupted:
            print(f'\nInterrupted before chunk {i+1}. Resume with --resume.')
            break

        print(f'Chunk {i+1}/{len(chunks)}: {chunk_start_str} to {chunk_end_str}')

        flux_query = build_flux_query(IFDB2_BUCKET, MEASUREMENT_NAME,
                                       chunk_start_str, chunk_end_str)
        try:
            t0 = time.time()
            record_stream = ifdbc2_query.query_stream(flux_query)
            process_stream(record_stream)
            t1 = time.time()
            print(f'  {total_records:,} records read, {total_written:,} written '
                  f'({t1 - t0:.1f}s)')

            # Save progress after each successful chunk
            save_progress(chunk_end_str, i, len(chunks),
                          total_records, total_written)
        except Exception as e:
            print(f'  Error in chunk: {e}', file=sys.stderr)
            continue

else:
    flux_query = build_flux_query(IFDB2_BUCKET, MEASUREMENT_NAME,
                                   start_time, end_time)

    print('Streaming data from InfluxDB 2...')
    if args.verbose:
        print(f'  Query: {flux_query.strip()}')

    try:
        t0 = time.time()
        record_stream = ifdbc2_query.query_stream(flux_query)
        process_stream(record_stream)
        t1 = time.time()
        print(f'  Streamed and processed in {t1 - t0:.1f}s')
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)

# Flush any remaining buffered writes in the InfluxDB 3 client
if ifdbc3:
    print('Flushing remaining writes...')
    ifdbc3.close()
    ifdbc3 = None

migration_elapsed = time.time() - migration_start

print()
if interrupted:
    print('Migration interrupted — progress saved.')
    print(f'  Resume with: {sys.argv[0]} --all --resume')
    print(f'  Progress file: {args.progress_file}')
else:
    print('Migration complete!')
    remove_progress()
print(f'  Total source records read:    {total_records:,}')
print(f'  Total destination records:    {total_written:,}')
print(f'  Batches:                      {batches_written:,}')
print(f'  Elapsed time:                 {migration_elapsed:.1f}s')
if total_written > 0 and migration_elapsed > 0:
    print(f'  Throughput:                   {total_written / migration_elapsed:,.0f} records/s')

if write_errors:
    print(f'\n  WARNING: {len(write_errors)} write error(s) occurred!', file=sys.stderr)
    for err in write_errors[:5]:
        print(f'    - {err}', file=sys.stderr)

if args.dryrun:
    print()
    print('[DRY RUN] No data was actually written to InfluxDB 3')
    if sample_record:
        print('  Sample record:')
        for k, v in sample_record.items():
            print(f'    {k}: {v}')

ifdbc2.close()
if ifdbc3:
    ifdbc3.close()
