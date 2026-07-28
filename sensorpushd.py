#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# sensorpushd.py, Copyright Bjoern Olausson
# -----------------------------------------------------------------------------
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# To view the license visit
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# or write to
# Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
# 02110-1301 USA
# -----------------------------------------------------------------------------
#
# Unified SensorPush daemon - queries the SensorPush API and stores
# temperature, humidity, and other sensor readings in InfluxDB 2,
# InfluxDB 3, and/or VictoriaMetrics.  Multiple backends can be
# active simultaneously.
#
# Can run as a one-shot command (backward compatible with cron) or as a
# continuous daemon managed by systemd.
#

import os
import sys
import json
import time
import math
import random
import signal
import socket
import logging
import tempfile
import threading
import requests
import datetime
import argparse
import configparser
from abc import ABC, abstractmethod
from pathlib import Path
from requests.adapters import HTTPAdapter


def suppress_insecure_warnings():
    """Silence urllib3's InsecureRequestWarning.

    Only called when a backend is actually configured with
    verify_ssl=False, so an unexpected certificate problem on a
    verified connection still surfaces instead of being hidden.
    """
    try:
        from urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    except Exception:
        pass


def enable_force_ipv4():
    """Force IPv4-only outbound connections for urllib3.

    On networks where IPv6 to api.sensorpush.com blackholes, a plain
    connect() stalls for minutes before falling back.  urllib3 reads
    HAS_IPV6 at call time, so flipping it here affects every later
    connection.  This is process-global, hence a single explicit call
    from main() rather than a constructor side effect.
    """
    try:
        import urllib3.util.connection
        urllib3.util.connection.HAS_IPV6 = False
        return True
    except Exception as e:
        logger.warning("Could not force IPv4: %s", e)
        return False


def sd_notify(state):
    """Send a notification to systemd (if available).

    Uses the NOTIFY_SOCKET environment variable set by systemd for
    Type=notify or WatchdogSec services.  Silently does nothing when
    the socket is not available (e.g. running outside systemd).

    The protocol is newline-delimited ``KEY=value``; callers passing
    free-form text (STATUS=) must sanitize it first - see sd_status().
    """
    sock_path = os.environ.get('NOTIFY_SOCKET')
    if not sock_path:
        return
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if sock_path.startswith('@'):
            sock_path = '\0' + sock_path[1:]
        sock.sendto(state.encode(), sock_path)
        sock.close()
    except Exception:
        pass


def sd_status(text):
    """Report a one-line human-readable status to systemd.

    Shows up in ``systemctl status`` so a degraded-but-running daemon
    is visible without trawling the journal.
    """
    sd_notify('STATUS=' + str(text).replace('\n', ' ')[:200])


logger = logging.getLogger('sensorpushd')

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
# api.sensorpush.com presents a valid public certificate chain, and the
# account password is sent in the auth request body - always verify it.
# The LAN backends have their own per-section verify_ssl settings.
VERIFY_SSL = True
RETRYWAIT = 60
MAXRETRY = 3
RETRY_DELAYS = [10, 30, 60, 120, 300]
# Stop feeding the systemd watchdog if the main loop makes no progress for
# this many seconds.  Must be larger than any single legitimate blocking
# step (an API call + its retries) but small enough to catch a real hang.
# Derived from WatchdogSec at runtime; this is the floor and the fallback.
WATCHDOG_HANG_THRESHOLD = 900
# Cap on the exponential backoff applied between failed collection cycles.
MAX_CYCLE_BACKOFF = 3600
# Log a failing cycle at CRITICAL (and report it to systemd) once this many
# consecutive failures have accumulated.
CYCLE_ALERT_AFTER = 12
# HTTP status codes worth retrying: transient server-side or rate limiting.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

API_URL_BASE = 'https://api.sensorpush.com/api/v1'
API_URL_OA_AUTH = f'{API_URL_BASE}/oauth/authorize'
API_URL_OA_ATOK = f'{API_URL_BASE}/oauth/accesstoken'
API_URL_GW = f'{API_URL_BASE}/devices/gateways'
API_URL_SE = f'{API_URL_BASE}/devices/sensors'
API_URL_SPL = f'{API_URL_BASE}/samples'
API_URL_RPL = f'{API_URL_BASE}/reports/list'
API_URL_RPDL = f'{API_URL_BASE}/reports/download'

HTTP_OA_HEAD = {'accept': 'application/json',
                'Content-Type': 'application/json'}

MEASURES = ["altitude", "barometric_pressure", "dewpoint", "humidity",
            "temperature", "vpd", "distance"]

# Defaults for the [DAEMON] section, shared by the unified and legacy loaders.
#
#   max_consecutive_failures  0 = never give up.  The daemon backs off
#       exponentially instead; a transient upstream outage must not end
#       collection permanently.
#   sensor_return_grace  How long a sensor that has started reporting again
#       must keep reporting before its gap is back-filled, so the gateway has
#       time to upload the sensor's buffered backlog to the cloud first.
#   sensor_stale_after  How far a sensor may fall behind the rest before it
#       is treated as missing rather than merely slow.  Sensors report on
#       their own cadence, so this must be well above normal jitter or every
#       cycle probes every sensor for nothing.
#   gap_lookback  How far back get_last_timestamp() looks when locating
#       existing data.  '0' = no limit.  Must exceed the longest outage the
#       daemon should still be able to recover from.
DEFAULT_DAEMON_CFG = {
    'interval': 300,
    'poll_backlog': '10m',
    'max_backfill': '0',
    'deep_scan_interval': '1h',
    'max_consecutive_failures': 0,
    'sensor_return_grace': '1h',
    'gap_lookback': '0',
    'sensor_stale_after': '6h',
}


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
def setup_logging(level='INFO', log_file=None):
    fmt = '%(asctime)s %(name)s %(levelname)s %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    handlers = []
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))
    logging.basicConfig(level=getattr(logging, level),
                        format=fmt, datefmt=datefmt,
                        handlers=handlers)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
def load_config(config_path):
    """Load config from file. Auto-detects legacy formats."""
    config = configparser.ConfigParser()

    if not Path(config_path).is_file():
        create_default_config(config_path)
        print(f'Created config file template at {config_path}')
        print('Please edit the config file with your settings and run again.')
        sys.exit(0)

    config.read(config_path)

    # Detect legacy InfluxDB 2 format (~/.sensorpush.conf)
    if 'INFLUXDBCONF' in config:
        return _load_legacy_influxdb2(config)

    # Detect legacy VictoriaMetrics format (~/.sensorpush_vm.conf)
    if 'VICTORIAMETRICSCONF' in config:
        return _load_legacy_vm(config)

    # New unified format (~/.sensorpushd.conf)
    return _load_unified(config)


def _load_legacy_influxdb2(config):
    """Load legacy ~/.sensorpush.conf format."""
    # Note: original config uses 'SONSORPUSHAPI' (typo preserved for compat)
    section = 'SONSORPUSHAPI' if 'SONSORPUSHAPI' in config else 'SENSORPUSHAPI'
    # Legacy config has separate IFDB_URL and IFDB_PORT — combine into one URL
    url = config['INFLUXDBCONF']['IFDB_URL']
    port = config['INFLUXDBCONF']['IFDB_PORT']
    host_part = url.split('://', 1)[-1]
    if ':' not in host_part:
        url = f'{url}:{port}'
    return {
        'login': config[section]['LOGIN'],
        'password': config[section]['PASSWD'],
        'backend': 'influxdb2',
        'my_altitude': float(config['MISC'].get('MY_ALTITUDE', '0')),
        'force_ipv4': config['MISC'].get('FORCE_IPv4', 'False').lower() in ('true', '1', 'yes'),
        'influxdb2': {
            'measurement_name': config['INFLUXDBCONF'].get('MEASUREMENT_NAME', 'SensorPush'),
            'url': url,
            'token': config['INFLUXDBCONF']['IFDB_TOKEN'],
            'org': config['INFLUXDBCONF']['IFDB_ORG'],
            'bucket': config['INFLUXDBCONF']['IFDB_BUCKET'],
            'verify_ssl': config['INFLUXDBCONF'].get('IFDB_VERIFY_SSL', 'False').lower() in ('true', '1', 'yes'),
        },
        'influxdb3': None,
        'victoriametrics': None,
        'daemon': dict(DEFAULT_DAEMON_CFG),
    }


def _load_legacy_vm(config):
    """Load legacy ~/.sensorpush_vm.conf format."""
    section = 'SONSORPUSHAPI' if 'SONSORPUSHAPI' in config else 'SENSORPUSHAPI'
    return {
        'login': config[section]['LOGIN'],
        'password': config[section]['PASSWD'],
        'backend': 'victoriametrics',
        'my_altitude': float(config['MISC'].get('MY_ALTITUDE', '0')),
        'force_ipv4': config['MISC'].getboolean('FORCE_IPv4', False),
        'influxdb2': None,
        'influxdb3': None,
        'victoriametrics': {
            'measurement_name': config['VICTORIAMETRICSCONF'].get('MEASUREMENT_NAME', 'SensorPush'),
            'url': config['VICTORIAMETRICSCONF'].get('VM_URL', 'http://localhost:8428'),
            'verify_ssl': config['VICTORIAMETRICSCONF'].getboolean('VM_VERIFY_SSL', False),
        },
        'daemon': dict(DEFAULT_DAEMON_CFG),
    }


def _load_unified(config):
    """Load new unified ~/.sensorpushd.conf format."""
    section = 'SONSORPUSHAPI' if 'SONSORPUSHAPI' in config else 'SENSORPUSHAPI'

    # Accept both 'PASSWD' (legacy) and 'PASSWORD' (new) key names
    if config.has_option(section, 'PASSWD'):
        password = config[section]['PASSWD']
    else:
        password = config[section]['PASSWORD']

    cfg = {
        'login': config.get(section, 'LOGIN', fallback=config.get(section, 'EMAIL', fallback='')),
        'password': password,
        'backend': config.get('BACKEND', 'TYPE', fallback='victoriametrics'),
        'my_altitude': config.getfloat('MISC', 'MY_ALTITUDE', fallback=0.0),
        'force_ipv4': config.getboolean('MISC', 'FORCE_IPv4', fallback=False),
        'influxdb2': None,
        'influxdb3': None,
        'victoriametrics': None,
        'daemon': {
            'interval': config.getint('DAEMON', 'INTERVAL', fallback=300),
            'poll_backlog': config.get('DAEMON', 'POLL_BACKLOG', fallback='10m'),
            'max_backfill': config.get('DAEMON', 'MAX_BACKFILL', fallback='0'),
            'deep_scan_interval': config.get('DAEMON', 'DEEP_SCAN_INTERVAL', fallback='1h'),
            'max_consecutive_failures': config.getint(
                'DAEMON', 'MAX_CONSECUTIVE_FAILURES', fallback=0),
            'sensor_return_grace': config.get(
                'DAEMON', 'SENSOR_RETURN_GRACE', fallback='1h'),
            'gap_lookback': config.get('DAEMON', 'GAP_LOOKBACK', fallback='0'),
            'sensor_stale_after': config.get(
                'DAEMON', 'SENSOR_STALE_AFTER', fallback='6h'),
        },
    }

    if 'INFLUXDB2' in config:
        url = config.get('INFLUXDB2', 'URL', fallback='http://localhost:8086')
        # Legacy compat: if PORT is set and URL doesn't already contain a port
        if config.has_option('INFLUXDB2', 'PORT'):
            port = config.get('INFLUXDB2', 'PORT')
            host_part = url.split('://', 1)[-1]
            if ':' not in host_part:
                url = f'{url}:{port}'
        cfg['influxdb2'] = {
            'measurement_name': config.get('INFLUXDB2', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': url,
            'token': config.get('INFLUXDB2', 'TOKEN', fallback=''),
            'org': config.get('INFLUXDB2', 'ORG', fallback=''),
            'bucket': config.get('INFLUXDB2', 'BUCKET', fallback='sensorpush'),
            'verify_ssl': config.getboolean('INFLUXDB2', 'VERIFY_SSL', fallback=False),
        }

    if 'INFLUXDB3' in config:
        # Read URL (new) or HOST (legacy) for backward compat
        if config.has_option('INFLUXDB3', 'URL'):
            url = config.get('INFLUXDB3', 'URL')
        else:
            url = config.get('INFLUXDB3', 'HOST', fallback='http://localhost:8181')
        cfg['influxdb3'] = {
            'measurement_name': config.get('INFLUXDB3', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': url,
            'database': config.get('INFLUXDB3', 'DATABASE', fallback='sensorpush'),
            'token': config.get('INFLUXDB3', 'TOKEN', fallback=''),
            'verify_ssl': config.getboolean('INFLUXDB3', 'VERIFY_SSL', fallback=False),
        }

    if 'VICTORIAMETRICS' in config:
        cfg['victoriametrics'] = {
            'measurement_name': config.get('VICTORIAMETRICS', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': config.get('VICTORIAMETRICS', 'URL', fallback='http://localhost:8428'),
            'verify_ssl': config.getboolean('VICTORIAMETRICS', 'VERIFY_SSL', fallback=False),
        }

    return cfg


def create_default_config(path):
    """Write a default config template."""
    config = configparser.ConfigParser()
    config['SENSORPUSHAPI'] = {
        'LOGIN': 'SensorPush login (email)',
        'PASSWORD': 'SensorPush password',
    }
    config['BACKEND'] = {
        'TYPE': 'victoriametrics',
    }
    config['INFLUXDB2'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'URL': 'http://localhost:8086',
        'TOKEN': 'your_influxdb2_token',
        'ORG': 'your_org',
        'BUCKET': 'sensorpush',
        'VERIFY_SSL': 'False',
    }
    config['INFLUXDB3'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'URL': 'http://localhost:8181',
        'DATABASE': 'sensorpush',
        'TOKEN': 'your_influxdb3_token',
        'VERIFY_SSL': 'False',
    }
    config['VICTORIAMETRICS'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'URL': 'http://localhost:8428',
        'VERIFY_SSL': 'False',
    }
    config['DAEMON'] = {
        'INTERVAL': '300',
        'POLL_BACKLOG': '10m',
        # 0 = back-fill without limit.  Raising this off 0 caps how far the
        # daemon can recover after a long outage - see GAP_LOOKBACK too.
        'MAX_BACKFILL': '0',
        'DEEP_SCAN_INTERVAL': '1h',
        # 0 = never stop collecting; back off exponentially instead.
        'MAX_CONSECUTIVE_FAILURES': '0',
        # Settle time before back-filling a sensor that started reporting again.
        'SENSOR_RETURN_GRACE': '1h',
        # 0 = search the full history when locating existing data.
        'GAP_LOOKBACK': '0',
        # How far a sensor may lag the others before it counts as missing.
        'SENSOR_STALE_AFTER': '6h',
    }
    config['MISC'] = {
        'MY_ALTITUDE': '0.0',
        # Set to True on networks where IPv6 to api.sensorpush.com blackholes
        # (symptom: connections hang for minutes instead of failing fast).
        'FORCE_IPv4': 'False',
    }
    with open(path, 'w') as f:
        config.write(f)


# -----------------------------------------------------------------------------
# CLI argument parsing
# -----------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description='Unified SensorPush daemon - queries the SensorPush API and '
                    'stores readings in InfluxDB 2, InfluxDB 3, and/or VictoriaMetrics')
    parser.add_argument(
        '-s', '--start', dest='starttime', default='', type=str,
        help='Start query at time (e.g. "2019-07-25T00:10:41+0200")')
    parser.add_argument(
        '-p', '--stop', dest='stoptime', default='', type=str,
        help='Stop query at time (e.g. "2019-07-26T00:10:41+0200")')
    parser.add_argument(
        '-b', '--backlog', dest='backlog', default='1d', type=str,
        help='Historical data to fetch (default 1d) - format: <number>[m|h|d|w|M|Y]')
    parser.add_argument(
        '-t', '--timestep', dest='timestep', default=720, type=int,
        help='Time slice per query in minutes (default 720 = 12h)')
    parser.add_argument(
        '-q', '--querylimit', dest='qlimit', default=0, type=int,
        help='Max samples per sensor (0 = unlimited, default 0)')
    parser.add_argument(
        '-d', '--delay', dest='delay', default=60, type=int,
        help='Delay in seconds between queries (default 60)')
    parser.add_argument(
        '-l', '--listsensors', dest='listsensors', action='store_true',
        help='Show a list of sensors and exit')
    parser.add_argument(
        '-g', '--listgateways', dest='listgateways', action='store_true',
        help='Show a list of gateways and exit')
    parser.add_argument(
        '-i', '--sensorlist', dest='sensorlist', nargs='+', type=str,
        help='List of sensor IDs to query')
    parser.add_argument(
        '-n', '--noconvert', dest='noconvert', action='store_true',
        help='Do not convert F to C, inHG to mBar, kPa to mBar and feet to meters')
    parser.add_argument(
        '-x', '--dryrun', dest='dryrun', action='store_true',
        help='Do not write anything to the database, just print what would have been written')
    parser.add_argument(
        '-v', '--verbose', dest='verbose', action='store_true',
        help='Show full output in dryrun mode (do not truncate)')
    # New arguments
    parser.add_argument(
        '--backend', dest='backend',
        nargs='+',
        choices=['influxdb2', 'influxdb3', 'victoriametrics'],
        default=None,
        help='Database backend(s) (overrides config file). '
             'Specify multiple to write to all simultaneously')
    parser.add_argument(
        '--daemon', dest='daemon', action='store_true', default=False,
        help='Run as a continuous daemon (default: one-shot mode)')
    parser.add_argument(
        '--interval', dest='interval', default=None, type=int,
        help='Polling interval in seconds for daemon mode (default: from config or 300)')
    parser.add_argument(
        '-c', '--config', dest='config', default=None, type=str,
        help='Path to config file (default: ~/.sensorpushd.conf)')
    parser.add_argument(
        '--log-level', dest='loglevel',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging verbosity (default: INFO)')
    parser.add_argument(
        '--log-file', dest='logfile', default=None, type=str,
        help='Log to file instead of stderr')
    parser.add_argument(
        '--generate-config', dest='generate_config', nargs='?',
        const='', default=None, metavar='PATH',
        help='Generate a default config file and exit. '
             'Optionally specify output path (default: ~/.sensorpushd.conf)')
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------
def local_time_offset(t=None):
    """Return offset of local zone from GMT, either at present or at time t."""
    if t is None:
        t = time.time()
    if time.localtime(t).tm_isdst and time.daylight:
        return -time.altzone / 3600
    else:
        return -time.timezone / 3600


def F_to_C(F, noconvert=False):
    if noconvert:
        return float(F)
    try:
        return float(round((F - 32) * 5.0 / 9.0, 2))
    except TypeError:
        return 0.0


def ft_to_m(ft, noconvert=False):
    if noconvert:
        return float(ft)
    return float(round(ft * 0.3048, 2))


def inHg_to_mBar(inHg, noconvert=False):
    if noconvert:
        return float(inHg)
    return float(round(inHg * 33.8639, 2))


def kPa_to_mBar(kPa, noconvert=False):
    if noconvert:
        return float(kPa)
    return float(round(kPa * 10, 2))


def parse_backlog(backlogstring):
    """Convert backlog string like '1d', '10m', '1M' to minutes."""
    minutes_per_unit = {
        "m": 1, "h": 60, "d": 60 * 24, "w": 60 * 24 * 7,
        "M": 60 * 24 * 30.417, "Y": 60 * 24 * 365
    }
    return int(int(backlogstring[:-1]) * minutes_per_unit[backlogstring[-1]])


def build_timelist(starttime, stoptime, timesteps):
    """Build list of [start, stop] time windows with 30-min overlaps."""
    timelist = []
    newstartt = None
    while starttime <= stoptime:
        start = datetime.date.strftime(starttime, '%Y-%m-%dT%X%z')
        if newstartt is not None:
            nextstop = newstartt + datetime.timedelta(minutes=int(timesteps))
        else:
            nextstop = starttime + datetime.timedelta(minutes=int(timesteps))
        stop = datetime.date.strftime(nextstop, '%Y-%m-%dT%X%z')
        starttime = nextstop - datetime.timedelta(minutes=30)
        newstartt = nextstop
        timelist.append([start, stop])
    return timelist


def parse_timestamp_to_ms(timestamp):
    """Convert timestamp string to milliseconds for VictoriaMetrics."""
    if isinstance(timestamp, str):
        try:
            dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            dt = datetime.datetime.strptime(timestamp.split('.')[0], '%Y-%m-%dT%H:%M:%S')
        return int(dt.timestamp() * 1000)
    return int(timestamp * 1000)


# -----------------------------------------------------------------------------
# Backend Writers
# -----------------------------------------------------------------------------
class BaseWriter(ABC):
    """Abstract base class for database backend writers."""

    def __init__(self):
        self._connected = False
        self._consecutive_failures = 0

    @property
    def connected(self):
        return self._connected

    @abstractmethod
    def connect(self):
        """Establish connection to the backend."""
        pass

    @abstractmethod
    def write(self, records):
        """Write a list of measurement dicts to the backend.

        Each record is: {'measurement': str, 'tags': dict, 'fields': dict, 'time': str}
        """
        pass

    @abstractmethod
    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        """Query the backend for the most recent data point timestamp.

        If sensor_id is given, returns the last timestamp for that
        specific sensor (using the temperature metric as reference).
        ``lookback_min`` bounds how far back to search; 0 searches the
        full history.  A bound shorter than the actual gap makes the
        sensor look like it has no data at all, which silently disables
        back-filling - so the caller derives it from max_backfill.
        Returns a datetime object or None if no data exists.
        """
        pass

    @abstractmethod
    def close(self):
        """Close the backend connection."""
        pass

    def reconnect(self):
        """Tear down and re-establish the connection."""
        try:
            self.close()
        except Exception:
            pass
        self._connected = False
        self.connect()


class InfluxDB2Writer(BaseWriter):
    """InfluxDB 2.x backend writer."""

    def __init__(self, config):
        super().__init__()
        self.url = config['url']
        self.token = config['token']
        self.org = config['org']
        self.bucket = config['bucket']
        self.verify_ssl = config['verify_ssl']
        self.measurement_name = config['measurement_name']
        self.client = None
        self.write_api = None
        self.query_api = None

    def connect(self):
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
        except ImportError:
            raise ImportError(
                "InfluxDB 2 backend requires the 'influxdb-client' package. "
                "Install with: pip install influxdb-client"
            )
        self.client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            verify_ssl=self.verify_ssl,
            timeout=15_000  # ms; the library default (10s) is undeclared
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self._connected = True
        logger.info("Connected to InfluxDB 2 at %s", self.url)

    def write(self, records):
        if not records:
            return
        self.write_api.write(bucket=self.bucket, org=self.org, record=records)

    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        try:
            sensor_filter = ''
            if sensor_id is not None:
                sensor_filter = (
                    f' |> filter(fn: (r) => r.sensor_id == "{sensor_id}")'
                )
            # 0 = unbounded: start from the Unix epoch rather than a
            # relative window, so a sensor stale for longer than the
            # window is still found.
            start = f'-{int(lookback_min)}m' if lookback_min else '1970-01-01T00:00:00Z'
            # last() runs per series, which is index-cheap.  A sensor that
            # was ever renamed has one series per name under the same
            # sensor_id, so this returns several rows - the caller takes the
            # newest.  (Collapsing with group()+sort() in Flux instead would
            # sort the entire history: ~26s across 18 sensors versus ~1s.)
            query = (
                f'from(bucket: "{self.bucket}")'
                f' |> range(start: {start})'
                f' |> filter(fn: (r) => r._measurement == "{measurement_name}"'
                f'    and r._field == "temperature")'
                f'{sensor_filter}'
                f' |> last()'
            )
            tables = self.query_api.query(query, org=self.org)
            newest = None
            for table in tables:
                for record in table.records:
                    ts = record.get_time()
                    if ts is not None and (newest is None or ts > newest):
                        newest = ts
            if newest is not None:
                return newest
        except Exception as e:
            logger.warning("Failed to query last timestamp from InfluxDB 2: %s", e)
        return None

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self.write_api = None
            self.query_api = None
            self._connected = False
            logger.info("InfluxDB 2 connection closed")


class InfluxDB3Writer(BaseWriter):
    """InfluxDB 3.x backend writer."""

    def __init__(self, config):
        super().__init__()
        self.url = config['url']
        self.database = config['database']
        self.token = config['token']
        self.verify_ssl = config.get('verify_ssl', False)
        self.measurement_name = config['measurement_name']
        self.client = None

    def connect(self):
        try:
            from influxdb_client_3 import InfluxDBClient3, write_client_options, SYNCHRONOUS
        except ImportError:
            raise ImportError(
                "InfluxDB 3 backend requires the 'influxdb3-python' package. "
                "Install with: pip install influxdb3-python"
            )
        wco = write_client_options(write_options=SYNCHRONOUS)
        self.client = InfluxDBClient3(
            host=self.url,
            database=self.database,
            token=self.token,
            write_client_options=wco,
            verify_ssl=self.verify_ssl
        )
        self._connected = True
        logger.info("Connected to InfluxDB 3 at %s", self.url)

    def write(self, records):
        if not records:
            return
        self.client.write(record=records)

    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        try:
            sensor_filter = ''
            if sensor_id is not None:
                sensor_filter = f" AND sensor_id = '{sensor_id}'"
            # 0 = unbounded: omit the time predicate entirely.
            if lookback_min:
                where = (f" WHERE time > now() - INTERVAL '{int(lookback_min)} minutes'"
                         f'{sensor_filter}')
            elif sensor_id is not None:
                where = f" WHERE sensor_id = '{sensor_id}'"
            else:
                where = ''
            query = (
                f'SELECT max(time) AS last_time FROM "{measurement_name}"'
                f'{where}'
            )
            result = self.client.query(query)
            if result and len(result) > 0:
                last_time = result.column('last_time')[0].as_py()
                if last_time:
                    return last_time
        except Exception as e:
            logger.warning("Failed to query last timestamp from InfluxDB 3: %s", e)
        return None

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self._connected = False
            logger.info("InfluxDB 3 connection closed")


class VMWriter(BaseWriter):
    """VictoriaMetrics backend writer using native JSON import API."""

    def __init__(self, config):
        super().__init__()
        self.url = config['url']
        self.verify_ssl = config['verify_ssl']
        self.measurement_name = config['measurement_name']
        self.session = None

    def connect(self):
        self.session = requests.Session()
        self._connected = True
        logger.info("VictoriaMetrics writer ready for %s", self.url)

    def write(self, records):
        if not records:
            return
        lines = []
        for record in records:
            lines.extend(self._to_json_lines(
                record['measurement'], record['tags'],
                record['fields'], record['time']
            ))
        if not lines:
            return
        data = '\n'.join(lines)
        url = f'{self.url}/api/v1/import'
        response = self.session.post(
            url,
            data=data.encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            verify=self.verify_ssl,
            timeout=15
        )
        response.raise_for_status()

    def _to_json_lines(self, measurement_name, tags, fields, timestamp):
        """Convert to VictoriaMetrics native JSON format."""
        timestamp_ms = parse_timestamp_to_ms(timestamp)
        lines = []
        for field_name, field_value in fields.items():
            metric = {'__name__': f'{measurement_name}_{field_name}'}
            for tag_key, tag_value in tags.items():
                metric[tag_key] = str(tag_value)
            json_obj = {
                'metric': metric,
                'values': [float(field_value)],
                'timestamps': [timestamp_ms]
            }
            lines.append(json.dumps(json_obj, ensure_ascii=False))
        return lines

    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        try:
            # Use last_over_time() which is standard MetricsQL.  The response
            # timestamp in the [value] array tells us when the last point was.
            sensor_filter = ''
            if sensor_id is not None:
                sensor_filter = f'sensor_id="{sensor_id}"'
            url = f'{self.url}/api/v1/query'
            metric = f'{measurement_name}_temperature'
            # MetricsQL has no unbounded lookbehind; 10y stands in for
            # "no limit" and comfortably exceeds any real retention.
            window = f'{int(lookback_min)}m' if lookback_min else '10y'
            query_str = f'last_over_time({metric}{{{sensor_filter}}}[{window}])'
            response = self.session.get(
                url,
                params={'query': query_str},
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            results = data.get('data', {}).get('result', [])
            if results:
                # One result per label set, so a renamed sensor yields
                # several series under the same sensor_id - take the
                # newest, not the first.  [0] = unix timestamp, [1] = value
                ts = max(float(r['value'][0]) for r in results)
                return datetime.datetime.fromtimestamp(
                    ts, tz=datetime.timezone.utc)
            return None
        except Exception as e:
            logger.warning("Failed to query last timestamp from VictoriaMetrics: %s", e)
        return None

    def close(self):
        if self.session:
            self.session.close()
            self.session = None
            self._connected = False
            logger.info("VictoriaMetrics session closed")


def create_writer(backend_name, config):
    """Factory: create the appropriate backend writer."""
    if backend_name == 'influxdb2':
        if not config.get('influxdb2'):
            raise ValueError(
                "InfluxDB 2 backend selected but [INFLUXDB2] config section is missing")
        return InfluxDB2Writer(config['influxdb2'])
    elif backend_name == 'influxdb3':
        if not config.get('influxdb3'):
            raise ValueError(
                "InfluxDB 3 backend selected but [INFLUXDB3] config section is missing")
        return InfluxDB3Writer(config['influxdb3'])
    elif backend_name == 'victoriametrics':
        if not config.get('victoriametrics'):
            raise ValueError(
                "VictoriaMetrics backend selected but [VICTORIAMETRICS] config section is missing")
        return VMWriter(config['victoriametrics'])
    else:
        raise ValueError(f"Unknown backend: {backend_name}")


def create_writers(backend_names, config):
    """Create writer instances for all specified backends."""
    writers = []
    for name in backend_names:
        writers.append(create_writer(name, config))
    return writers


# -----------------------------------------------------------------------------
# SensorPush API
# -----------------------------------------------------------------------------
class AuthCredentialsError(Exception):
    """The API rejected the credentials themselves.

    Distinct from a transient outage: retrying will not help, so callers
    should surface this rather than burning the retry budget on it.
    """


class SensorPushAPI:
    """Client for the SensorPush cloud API."""

    def __init__(self, login, password, verify_ssl=True, sleep_func=None):
        self.login = login
        self.password = password
        self.verify_ssl = verify_ssl
        # Injected by the daemon so retry sleeps stay interruptible and keep
        # feeding the watchdog.  Defaults to a plain sleep for one-shot use.
        self.sleep = sleep_func or time.sleep
        self.session = requests.Session()
        self.session.mount(API_URL_BASE, HTTPAdapter(max_retries=0))
        self.access_token = None
        self._token_time = None
        self._auth_header = None

    def _auth_post(self, url, data, what):
        """POST one step of the OAuth flow, retrying transient failures.

        Both auth steps go through here.  A 4xx other than 429 means the
        request itself is wrong (bad credentials, malformed body) and is
        raised immediately - only genuinely transient conditions consume
        retries, with exponential backoff plus jitter so a fleet of
        clients does not resynchronise on a recovering server.
        """
        last = None
        for attempt in range(1, MAXRETRY + 1):
            logger.info("%s - try %d/%d", what, attempt, MAXRETRY)
            try:
                r = self.session.post(url, headers=HTTP_OA_HEAD, data=data,
                                      verify=self.verify_ssl, timeout=30)
                if r.status_code == 200:
                    return r
                last = f"status {r.status_code}"
                if r.status_code in (401, 403):
                    raise AuthCredentialsError(
                        f"{what} rejected the credentials (status "
                        f"{r.status_code}) - check login/password")
                if r.status_code not in RETRYABLE_STATUS:
                    raise ConnectionError(f"{what} failed with status "
                                          f"{r.status_code}")
                logger.error("%s failed with status %d (retryable)",
                             what, r.status_code)
            except requests.exceptions.RequestException as e:
                last = str(e)
                logger.warning("Connection error during %s: %s", what, e)

            if attempt >= MAXRETRY:
                break
            # 20s, 40s, 80s ... capped, plus up to 25% jitter.
            delay = min(20 * 2 ** (attempt - 1), 160)
            delay = int(delay * (1 + random.random() * 0.25))
            logger.info("Retrying %s in %ds", what, delay)
            self.sleep(delay)

        raise ConnectionError(
            f"{what} failed after {MAXRETRY} attempts (last: {last})")

    def authenticate(self):
        """Perform the full OAuth authentication flow."""
        http_data = json.dumps({'email': self.login, 'password': self.password})

        r = self._auth_post(API_URL_OA_AUTH, http_data,
                            "Fetching API oauth authorization string")
        auth = r.content.decode('utf-8')

        r = self._auth_post(API_URL_OA_ATOK, auth,
                            "Fetching API oauth access token")
        self.access_token = json.loads(r.content.decode('utf-8'))['accesstoken']
        self._token_time = time.time()
        self._auth_header = {
            'accept': 'application/json',
            'Authorization': self.access_token
        }
        logger.info("Authentication successful")

    def _ensure_auth(self):
        """Re-authenticate if token is likely expired (>55 min)."""
        if (self._token_time is None or
                time.time() - self._token_time > 3300):
            self.authenticate()

    def _post(self, url, data=None):
        """Make an authenticated POST request.

        A 401/403 here means the cached token expired earlier than the
        55-minute assumption, so re-authenticate once and retry rather
        than failing the whole cycle.
        """
        self._ensure_auth()
        http_data = json.dumps(data or {})
        for attempt in (1, 2):
            r = self.session.post(url, headers=self._auth_header,
                                  data=http_data, verify=self.verify_ssl,
                                  timeout=60)
            if r.status_code == 200:
                return json.loads(r.content.decode('utf-8'))
            if r.status_code in (401, 403) and attempt == 1:
                logger.info("Token rejected by %s, re-authenticating", url)
                self._token_time = None
                self._ensure_auth()
                continue
            raise ValueError(
                f"API request to {url} failed with status {r.status_code}: "
                f"{r.content.decode('utf-8')}")

    def get_gateways(self):
        logger.info("Fetching the list of gateways")
        return self._post(API_URL_GW)

    def get_sensors(self):
        logger.info("Fetching the list of sensors")
        return self._post(API_URL_SE)

    def get_reports(self):
        logger.info("Fetching the list of bulk reports")
        return self._post(API_URL_RPL)

    def get_samples(self, start, stop, measures=None, limit=0, sensors=None):
        query = {'startTime': start, 'stopTime': stop,
                 'measures': measures or MEASURES}
        if limit != 0:
            query['limit'] = limit
        if sensors:
            query['sensors'] = sensors
        return self._post(API_URL_SPL, query)


# -----------------------------------------------------------------------------
# Data processing
# -----------------------------------------------------------------------------
def build_voltage_records(sensors, measurement_name, querytime):
    """Build voltage/RSSI measurement records for all sensors."""
    records = []
    measurement_v_name = f'{measurement_name}_V'
    for sid in sensors.keys():
        try:
            bat_volt = float(sensors[sid]["battery_voltage"])
        except (KeyError, TypeError):
            bat_volt = 0.0
        try:
            rssi = float(sensors[sid]["rssi"])
        except (KeyError, TypeError):
            rssi = 0.0

        records.append({
            'measurement': str(measurement_v_name),
            'tags': {
                'sensor_id': float(sensors[sid]["id"]),
                'sensor_name': str(sensors[sid]["name"]),
            },
            'fields': {
                'voltage': float(bat_volt),
                'rssi': float(rssi),
            },
            'time': datetime.date.strftime(querytime, '%Y-%m-%dT%X%z')
        })
    return records


def process_samples(samples, sensors, measurement_name, my_altitude, noconvert):
    """Process API sample response into measurement records."""
    records = []

    for key in samples['sensors'].keys():
        for item in samples['sensors'][key]:
            observed = str(item['observed'])
            sensor_name = str(sensors[key]['name'])

            m = {
                'measurement': str(measurement_name),
                'tags': {
                    'sensor_id': float(key),
                    'sensor_name': str(sensor_name),
                },
                'fields': {},
                'time': str(observed)
            }

            try:
                humidity = float(item['humidity'])
            except KeyError:
                pass
            else:
                m['fields']['humidity'] = float(humidity)

            try:
                temperature = F_to_C(item['temperature'], noconvert)
            except KeyError:
                pass
            else:
                m['fields']['temperature'] = float(temperature)

            try:
                pressure = inHg_to_mBar(item['barometric_pressure'], noconvert)
            except KeyError:
                # Absolute humidity (g/m3) - simplified formula without pressure
                # https://carnotcycle.wordpress.com/2012/08/04/how-to-convert-relative-humidity-to-absolute-humidity/
                abs_humidity = float(round(
                    (6.112 * math.e**((17.67 * temperature) / (temperature + 243.5))
                     * humidity * 2.1674) / (273.15 + temperature), 2))
                m['fields']['abs_humidity'] = float(abs_humidity)
            else:
                m['fields']['pressure'] = float(pressure)
                # Absolute humidity (g/m3) - accurate formula with pressure
                # https://www.loxwiki.eu/display/LOX/Absolute+Luftfeuchtigkeit+berechnen
                abs_humidity = float(round(
                    0.622 * humidity / 100 * (
                        1.01325 * 10.0**(5.426651 - 2005.1 / (temperature + 273.15)
                        + 0.00013869 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        / (temperature + 273.15) * (10.0**(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) - 1.0)
                        - 0.0044 * 10.0**((-0.0057148 * (374.11 - temperature)**1.25)))
                        + (((temperature + 273.15) / 647.3) - 0.422)
                        * (0.577 - ((temperature + 273.15) / 647.3))
                        * math.exp(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0))
                        * 0.00980665)
                    / (pressure / 1000.0 - humidity / 100.0 * (
                        1.01325 * 10.0**(5.426651 - 2005.1 / (temperature + 273.15)
                        + 0.00013869 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        / (temperature + 273.15) * (10.0**(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) - 1.0)
                        - 0.0044 * 10.0**((-0.0057148 * (374.11 - temperature)**1.25)))
                        + (((temperature + 273.15) / 647.3) - 0.422)
                        * (0.577 - ((temperature + 273.15) / 647.3))
                        * math.exp(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0))
                        * 0.00980665))
                    * pressure / 1000.0 * 100000000.0
                    / ((temperature + 273.15) * 287.1), 2))
                m['fields']['abs_humidity'] = float(abs_humidity)

            try:
                altitude = ft_to_m(item['altitude'], noconvert)
            except KeyError:
                altitude = float(my_altitude)
            finally:
                if altitude == 0:
                    altitude = float(my_altitude)
                m['fields']['altitude'] = float(altitude)

            try:
                distance = ft_to_m(item['distance'], noconvert)
            except KeyError:
                pass
            else:
                m['fields']['distance'] = float(distance)

            try:
                dewpoint = F_to_C(item['dewpoint'], noconvert)
            except KeyError:
                # Dewpoint in degree centigrade
                # https://cals.arizona.edu/azmet/dewpoint.html
                dewpoint = float(round(
                    (237.3 * ((math.log(humidity / 100) +
                    ((17.27 * temperature) / (237.3 + temperature))) / 17.27))
                    / (1 - ((math.log(humidity / 100) +
                    ((17.27 * temperature) / (237.3 + temperature))) / 17.27)), 2))
            finally:
                m['fields']['dewpoint'] = float(dewpoint)

            try:
                vpd = kPa_to_mBar(item['vpd'], noconvert)
            except KeyError:
                # Vapor Pressure Deficit in mBar
                # https://pulsegrow.com/blogs/learn/vpd
                vpd = float(kPa_to_mBar(
                    ((610.78 * math.e**(temperature / (temperature + 238.3) * 17.2694))
                     / 1000) * (1 - humidity / 100), noconvert))
            finally:
                m['fields']['vpd'] = float(vpd)

            records.append(m)

    return records


# -----------------------------------------------------------------------------
# Daemon
# -----------------------------------------------------------------------------
class SensorPushDaemon:
    """Main daemon: handles collection cycles, retries, and gap-filling."""

    def __init__(self, api, writers, config, args):
        self.api = api
        self.writers = writers
        self.config = config
        self.args = args
        self.running = True
        self._last_successful_time = None
        self._heartbeat = time.monotonic()
        self._watchdog_thread = None
        self._hang_threshold = WATCHDOG_HANG_THRESHOLD
        # Process exit status.  Set to non-zero on any abnormal exit so
        # systemd's Restart=on-failure/always actually sees a failure - a
        # clean exit(0) here silently ends collection until someone notices.
        self._exit_code = 0
        # Throttling of the expensive per-sensor "deep" gap scan.
        # _fetch_high_water is the stop time of the last completed cycle;
        # _last_deep_scan is a monotonic timestamp of the last deep scan.
        self._fetch_high_water = None
        self._last_deep_scan = None
        # Per-sensor return tracking, persisted across restarts:
        #   {sensor_id: {'returned_at': <epoch>, 'gap_from': <iso8601>}}
        # A sensor that has started reporting again is recorded here and
        # back-filled only after sensor_return_grace has elapsed.
        self._sensor_state = {}
        # Filled in by the deep scan, consumed by _handle_sensor_returns().
        self._sensor_last_stored = {}
        self._collector_high_water = None
        # Windows whose fetch failed outright.  They are retried on later
        # cycles; until they succeed the high-water mark must not advance
        # past them or the hole becomes invisible to gap detection.
        self._failed_windows = []
        self._state_path = self._resolve_state_path()
        self._load_state()
        self._setup_signals()

    # -- persistent state ---------------------------------------------------

    def _resolve_state_path(self):
        """Pick a writable location for the sensor-return state file.

        Prefers systemd's StateDirectory=, which stays writable under
        ProtectSystem=strict.  Falls back to the XDG state dir for manual
        runs.  Returns None if nothing is writable - the daemon then runs
        with in-memory state only, which just means a restart re-arms the
        grace period rather than losing data.
        """
        candidates = []
        sd_state = os.environ.get('STATE_DIRECTORY')
        if sd_state:
            candidates.append(Path(sd_state.split(':')[0]))
        xdg = os.environ.get('XDG_STATE_HOME')
        candidates.append(Path(xdg) / 'sensorpushd' if xdg
                          else Path.home() / '.local' / 'state' / 'sensorpushd')
        for base in candidates:
            try:
                base.mkdir(parents=True, exist_ok=True)
                probe = base / '.write-test'
                probe.touch()
                probe.unlink()
                return base / 'state.json'
            except Exception:
                continue
        logger.warning("No writable state directory found; sensor-return "
                       "tracking will not survive a restart")
        return None

    def _load_state(self):
        if not self._state_path or not self._state_path.is_file():
            return
        try:
            with open(self._state_path) as f:
                data = json.load(f)
            self._sensor_state = data.get('sensors', {})
            self._failed_windows = data.get('failed_windows', [])
            if self._sensor_state or self._failed_windows:
                logger.info("Restored state: %d sensor(s) pending back-fill, "
                            "%d failed window(s) to retry",
                            len(self._sensor_state), len(self._failed_windows))
        except Exception as e:
            logger.warning("Could not read state file %s: %s",
                           self._state_path, e)

    def _write_last_success(self):
        """Publish the last time data actually reached a backend.

        Process supervision cannot see the failure mode that matters most
        - a daemon that is alive but storing nothing - so this timestamp
        is what the freshness check alerts on.  Only written when a write
        genuinely succeeded, never merely when a cycle did not raise.
        """
        if not self._state_path:
            return
        try:
            path = self._state_path.parent / 'last-success'
            tmp = path.with_suffix('.tmp')
            tmp.write_text(self._last_successful_time.isoformat() + '\n')
            os.replace(tmp, path)
        except Exception as e:
            logger.debug("Could not write last-success marker: %s", e)

    def _save_state(self):
        """Persist state atomically so a crash mid-write cannot corrupt it."""
        if not self._state_path:
            return
        payload = {
            'sensors': self._sensor_state,
            'failed_windows': self._failed_windows,
        }
        try:
            fd, tmp = tempfile.mkstemp(dir=str(self._state_path.parent),
                                       prefix='.state-', suffix='.tmp')
            with os.fdopen(fd, 'w') as f:
                json.dump(payload, f)
            os.replace(tmp, self._state_path)
        except Exception as e:
            logger.warning("Could not write state file %s: %s",
                           self._state_path, e)

    def _setup_signals(self):
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGHUP, self._handle_signal)

    def _handle_signal(self, signum, frame):
        if signum in (signal.SIGTERM, signal.SIGINT):
            logger.info("Received %s, shutting down gracefully...",
                        signal.Signals(signum).name)
            sd_notify('STOPPING=1')
            self.running = False
        elif signum == signal.SIGHUP:
            logger.info("Received SIGHUP, will reload configuration on next cycle")

    def _touch(self):
        """Record forward progress for the watchdog heartbeat.

        A plain float assignment is atomic under the GIL, so no lock is
        needed.  Called whenever the daemon does meaningful work (each
        cycle, each fetch iteration, each second of sleep) so the
        background feeder knows the main loop is alive.
        """
        self._heartbeat = time.monotonic()

    def _start_watchdog(self):
        """Feed the systemd watchdog from a background thread.

        The feeder pings systemd only while the main loop keeps making
        progress (see _touch).  This lets a long but healthy operation -
        e.g. backfilling days or weeks of data for a sensor that has been
        offline - run to completion without being killed, while a process
        that is genuinely stuck stops being pinged and gets restarted by
        systemd.  Does nothing when not running under a watchdog.
        """
        wd_usec = os.environ.get('WATCHDOG_USEC')
        if not wd_usec:
            logger.debug("No systemd watchdog configured (WATCHDOG_USEC unset)")
            return
        try:
            wd_sec = int(wd_usec) / 1_000_000.0
        except ValueError:
            logger.warning("Invalid WATCHDOG_USEC=%r, watchdog feeder disabled",
                           wd_usec)
            return
        ping_interval = max(1.0, wd_sec / 3.0)
        # Derive the hang threshold from WatchdogSec so tightening the unit
        # tightens real hang detection.  Floored so a legitimate long step
        # (an API call plus its retries) is never mistaken for a hang.
        self._hang_threshold = max(WATCHDOG_HANG_THRESHOLD, wd_sec * 0.75)

        def _loop():
            while self.running:
                stale = time.monotonic() - self._heartbeat
                if stale < self._hang_threshold:
                    sd_notify('WATCHDOG=1')
                else:
                    logger.error(
                        "Watchdog heartbeat stale for %.0fs (>%.0fs) - not "
                        "pinging systemd; the daemon will be restarted",
                        stale, self._hang_threshold)
                time.sleep(ping_interval)

        self._watchdog_thread = threading.Thread(
            target=_loop, name="watchdog-feeder", daemon=True)
        self._watchdog_thread.start()
        logger.info(
            "Watchdog feeder started (timeout=%.0fs, ping every %.0fs, "
            "hang threshold=%.0fs)",
            wd_sec, ping_interval, self._hang_threshold)

    def _get_measurement_name(self):
        """Get the measurement name from the first active backend."""
        if self.writers:
            return self.writers[0].measurement_name
        return 'SensorPush'

    def _interruptible_sleep(self, seconds):
        """Sleep that can be interrupted by signals.

        Updates the watchdog heartbeat every second so the background
        feeder keeps the systemd timer satisfied during idle periods
        (between cycles and during retry backoffs).
        """
        for i in range(int(seconds)):
            if not self.running:
                return
            self._touch()
            time.sleep(1)

    def run(self):
        """Main daemon loop.

        Returns the process exit status.  A collection failure never ends
        the daemon by default: the upstream API can be down for hours, and
        a collector that gives up permanently turns a transient outage into
        an indefinite one.  Instead the retry interval backs off
        exponentially (capped) and the failure is escalated to CRITICAL and
        reported to systemd once it stops looking transient.
        """
        interval = self.args.interval or self.config['daemon']['interval']
        max_failures = int(self.config['daemon'].get(
            'max_consecutive_failures', 0) or 0)
        logger.info("Starting SensorPush daemon (interval=%ds, "
                    "give up after=%s)", interval,
                    max_failures if max_failures > 0 else 'never')

        # Connect to all backends (with retry)
        self._connect_writers()

        sd_notify('READY=1')
        sd_status('Starting first collection cycle')
        self._start_watchdog()

        consecutive_failures = 0

        while self.running:
            self._touch()
            sleep_for = interval
            try:
                self._collect_cycle()
                consecutive_failures = 0
                sd_status('OK: last successful cycle %s' %
                          self._last_successful_time)
            except Exception as e:
                consecutive_failures += 1
                # Exponential backoff, capped, so a multi-hour upstream
                # outage costs a handful of requests rather than thousands.
                sleep_for = min(interval * 2 ** min(consecutive_failures - 1, 8),
                                MAX_CYCLE_BACKOFF)
                log = (logger.critical if consecutive_failures >= CYCLE_ALERT_AFTER
                       else logger.error)
                log("Collection cycle failed (%d consecutive), retrying in "
                    "%ds: %s", consecutive_failures, sleep_for, e)
                sd_status('DEGRADED: %d consecutive failures: %s'
                          % (consecutive_failures, e))
                if max_failures > 0 and consecutive_failures >= max_failures:
                    logger.critical(
                        "Too many consecutive failures (%d), exiting",
                        consecutive_failures)
                    sd_status('FATAL: giving up after %d consecutive failures'
                              % consecutive_failures)
                    self._exit_code = 1
                    self.running = False
                    break

            self._interruptible_sleep(sleep_for)

        self._save_state()
        for w in self.writers:
            try:
                w.close()
            except Exception as e:
                logger.warning("Error closing %s: %s", type(w).__name__, e)
        logger.info("Daemon stopped")
        return self._exit_code

    def _connect_writers(self):
        """Connect all backend writers with retries. Fails only if ALL fail.

        Backends that fail to connect are kept in self.writers with
        _connected=False so that _safe_write() can attempt reconnection
        on every subsequent cycle.
        """
        failed = []
        for writer in self.writers:
            for attempt, delay in enumerate(RETRY_DELAYS, 1):
                try:
                    writer.connect()
                    break
                except Exception as e:
                    logger.error("Failed to connect %s (attempt %d/%d): %s",
                                 type(writer).__name__, attempt,
                                 len(RETRY_DELAYS), e)
                    if attempt < len(RETRY_DELAYS):
                        logger.info("Retrying in %ds...", delay)
                        self._interruptible_sleep(delay)
            if not writer.connected:
                failed.append(type(writer).__name__)
        if len(failed) == len(self.writers):
            raise ConnectionError(
                "Failed to connect to any backend: " + ', '.join(failed))
        if failed:
            logger.warning("Some backends unavailable at startup: %s. "
                           "Will keep retrying on each cycle.",
                           ', '.join(failed))

    def _collect_cycle(self):
        """One collection cycle: auth, fetch, process, write."""
        mytz = datetime.timezone(datetime.timedelta(hours=local_time_offset()))
        currenttime = datetime.datetime.now(tz=mytz)
        measurement_name = self._get_measurement_name()

        # Authenticate, reusing the cached token while it is still valid.
        # Re-authenticating every cycle multiplied exposure to auth outages
        # and cost ~11x the requests for no benefit.
        self.api._ensure_auth()

        # Fetch and write voltage/RSSI data
        sensors = self.api.get_sensors()
        logger.info("Building voltage records for %d sensors", len(sensors))
        voltage_records = build_voltage_records(
            sensors, measurement_name, currenttime)
        if self.args.dryrun:
            self._log_dryrun(voltage_records)
        else:
            logger.info("Writing %d voltage records to backends",
                        len(voltage_records))
            self._safe_write(voltage_records)

        # Fetch bulk reports (informational)
        try:
            reports = self.api.get_reports()
            if len(reports.get("files", [])) > 0:
                logger.info("Bulk reports available: %d files",
                            len(reports["files"]))
        except Exception as e:
            logger.warning("Failed to fetch reports: %s", e)

        # Determine time window (gap-filling in daemon mode)
        deep_scan = False
        if self.args.daemon:
            starttime, stoptime, deep_scan = self._compute_daemon_window(
                mytz, currenttime, sensors)
        else:
            starttime, stoptime = self._compute_oneshot_window(
                mytz, currenttime)

        # Re-drive any window that previously failed outright, before
        # advancing anything - otherwise the hole is never revisited.
        if self.args.daemon:
            self._retry_failed_windows(sensors, measurement_name)

        timelist = build_timelist(starttime, stoptime, self.args.timestep)
        iterations = len(timelist)

        logger.info("Start: %s",
                     datetime.date.strftime(starttime, '%Y-%m-%dT%X%z'))
        logger.info("Stop:  %s",
                     datetime.date.strftime(stoptime, '%Y-%m-%dT%X%z'))
        logger.info("Iterations required: %d", iterations)

        # Fetch and write samples
        all_ok = True
        wrote_any = False
        for i, window in enumerate(timelist, 1):
            if not self.running:
                logger.info("Shutdown requested, stopping collection")
                all_ok = False
                break
            if self._fetch_and_write_window(
                    window, i, iterations, sensors, measurement_name):
                wrote_any = True
            else:
                all_ok = False
                self._record_failed_window(window)
        else:
            # Loop ran to completion (not aborted by shutdown).  Only
            # advance the high-water mark if every window actually landed;
            # advancing past a failed window hides it from gap detection,
            # which only ever looks at the newest stored point.
            if self.args.daemon and all_ok:
                self._fetch_high_water = stoptime

        if wrote_any:
            self._last_successful_time = datetime.datetime.now(
                tz=datetime.timezone.utc)
            self._write_last_success()

        # Sensors lagging behind the rest are handled separately, and only
        # on deep-scan cycles to keep the probe traffic bounded.
        if self.args.daemon and deep_scan:
            self._handle_sensor_returns(sensors, measurement_name, currenttime)
            self._save_state()

    def _gap_lookback_min(self):
        """How far back gap-detection queries should search.

        Must be at least as long as the gap being recovered, or the
        lagging sensor looks like it has no data at all and back-filling
        is silently skipped.  Derived from max_backfill (plus a wide
        margin) so the two settings cannot disagree; 0 means unbounded.
        """
        cfg = self.config['daemon'].get('gap_lookback', '0')
        if cfg and cfg != '0':
            return parse_backlog(cfg)
        max_backfill_str = self.config['daemon']['max_backfill']
        if max_backfill_str and max_backfill_str != '0':
            return parse_backlog(max_backfill_str) * 2
        return 0

    def _compute_daemon_window(self, mytz, currenttime, sensors):
        """Compute time window for daemon mode.

        Returns ``(starttime, stoptime, deep_scan_ran)``.

        Two modes:

        * Deep scan - queries every sensor on every backend to find how
          far behind the *collector* is, i.e. the newest point each
          backend holds.  Taking the newest (not the oldest) is what keeps
          a single dead sensor from dragging the window across the entire
          history on every scan; sensors lagging behind the pack are dealt
          with individually by _handle_sensor_returns().
        * Incremental poll - between deep scans, simply continue from the
          high-water mark of the last completed cycle.  This keeps online
          sensors current cheaply.
        """
        poll_backlog_str = self.config['daemon']['poll_backlog']
        poll_backlog_min = parse_backlog(poll_backlog_str)
        max_backfill_str = self.config['daemon']['max_backfill']
        max_backfill_min = parse_backlog(max_backfill_str) if max_backfill_str != '0' else 0
        deep_scan_str = self.config['daemon'].get('deep_scan_interval', '1h')
        deep_scan_min = parse_backlog(deep_scan_str) if deep_scan_str != '0' else 0
        lookback_min = self._gap_lookback_min()

        # Decide whether a deep scan is due.  Always deep-scan on the first
        # cycle (no high-water mark yet) and when throttling is disabled.
        now_mono = time.monotonic()
        deep_due = (
            self._fetch_high_water is None
            or self._last_deep_scan is None
            or deep_scan_min <= 0
            or (now_mono - self._last_deep_scan) >= deep_scan_min * 60
        )

        if not deep_due:
            # Incremental poll from the last completed cycle, with a small
            # overlap, but never a shorter window than poll_backlog.
            overlap = datetime.timedelta(minutes=2)
            fast_start = self._fetch_high_water - overlap
            backlog_start = currenttime - datetime.timedelta(
                minutes=poll_backlog_min)
            starttime = min(fast_start, backlog_start)
            mins_to_deep = deep_scan_min - (now_mono - self._last_deep_scan) / 60
            logger.info(
                "Incremental poll from %s (deep scan in ~%.0f min)",
                starttime.strftime('%Y-%m-%dT%X%z'), max(0, mins_to_deep))
            return starttime, currenttime, False

        # --- deep scan ---
        self._last_deep_scan = now_mono
        logger.info("Running deep gap scan (deep_scan_interval=%s)",
                    deep_scan_str)

        # Build sensor ID list (same IDs used as tags in records)
        sensor_ids = list(sensors.keys())

        # per_sensor[sid]  = oldest "last seen" across backends, i.e. the
        #                    point from which that sensor still needs data
        # per_writer[name] = newest "last seen" on that backend, i.e. how
        #                    current the backend as a whole is
        per_sensor = {}
        per_writer = {}

        for writer in self.writers:
            wname = type(writer).__name__
            if not writer.connected or writer._consecutive_failures > 0:
                if not writer.connected:
                    logger.debug("Skipping gap check on %s (not connected)",
                                 wname)
                else:
                    logger.debug("Skipping gap check on %s "
                                 "(%d consecutive write failures)",
                                 wname, writer._consecutive_failures)
                continue
            for sid in sensor_ids:
                if not self.running:
                    break
                self._touch()
                # sensor_id tag is stored as float(key), e.g. "1234567.0"
                sid_tag = str(float(sid))
                try:
                    ts = writer.get_last_timestamp(
                        writer.measurement_name, sensor_id=sid_tag,
                        lookback_min=lookback_min)
                except Exception as e:
                    logger.warning(
                        "Could not query last timestamp for sensor %s "
                        "from %s: %s", sid, wname, e)
                    continue
                if ts is None:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                if sid not in per_sensor or ts < per_sensor[sid]:
                    per_sensor[sid] = ts
                if wname not in per_writer or ts > per_writer[wname]:
                    per_writer[wname] = ts

        # Remember for _handle_sensor_returns(); local tz for logging.
        self._sensor_last_stored = {
            sid: ts.astimezone(mytz) for sid, ts in per_sensor.items()}

        # The collector is only as current as its most-behind backend.
        collector_ts = min(per_writer.values()) if per_writer else None
        self._collector_high_water = (collector_ts.astimezone(mytz)
                                      if collector_ts else None)

        if collector_ts:
            collector_local = collector_ts.astimezone(mytz)
            gap_minutes = (currenttime - collector_local).total_seconds() / 60
            if gap_minutes > poll_backlog_min:
                behind = min(per_writer, key=per_writer.get)
                logger.info(
                    "Collector gap: %s last stored data at %s (%.0f min ago), "
                    "fetching with 1h safety margin",
                    behind, collector_local.strftime('%Y-%m-%dT%X%z'),
                    gap_minutes)
                # Go back 1 hour before the last stored point as safety margin
                starttime = collector_local - datetime.timedelta(hours=1)
            else:
                starttime = currenttime - datetime.timedelta(
                    minutes=poll_backlog_min)
        else:
            logger.info("No existing data found, using poll backlog of %s",
                        poll_backlog_str)
            starttime = currenttime - datetime.timedelta(
                minutes=poll_backlog_min)

        # Cap how far back we go if max_backfill is set
        if max_backfill_min > 0:
            earliest_allowed = currenttime - datetime.timedelta(
                minutes=max_backfill_min)
            if starttime < earliest_allowed:
                logger.warning(
                    "Capping backfill start from %s to %s (max_backfill=%s) - "
                    "data older than the cap will not be recovered",
                    starttime.strftime('%Y-%m-%dT%X%z'),
                    earliest_allowed.strftime('%Y-%m-%dT%X%z'),
                    max_backfill_str)
                starttime = earliest_allowed

        return starttime, currenttime, True

    def _handle_sensor_returns(self, sensors, measurement_name, currenttime):
        """Back-fill sensors that have started reporting again.

        A sensor can be out of range for weeks.  Its gateway only uploads
        the buffered backlog once the sensor is back in range, and that
        upload is not instant - so back-filling the moment the first new
        sample appears would fetch a window the cloud has not finished
        populating.  Instead: notice the return, wait
        ``sensor_return_grace``, then fetch the sensor's whole gap once.

        A sensor that is simply dead (flat battery) never produces new
        data, so it costs exactly one cheap probe per deep scan and never
        triggers a back-fill - which is what stops one dead sensor from
        forcing a full-history refetch of everything, every hour.

        The gap is recorded as soon as a sensor falls behind, *before* it
        returns.  Gap detection reads the newest stored point, so the
        moment a sensor resumes, the ordinary incremental poll stores a
        fresh sample and that point jumps to the present - the sensor
        stops looking stale and the hole behind it becomes invisible.
        Remembering the gap up front is what keeps a returning sensor's
        backlog recoverable.
        """
        per_sensor = getattr(self, '_sensor_last_stored', {})
        collector_hw = getattr(self, '_collector_high_water', None)
        if not per_sensor or collector_hw is None:
            return

        stale_str = self.config['daemon'].get('sensor_stale_after', '6h')
        stale_after_min = parse_backlog(stale_str)
        grace_str = self.config['daemon'].get('sensor_return_grace', '1h')
        grace_sec = parse_backlog(grace_str) * 60 if grace_str != '0' else 0
        now = time.time()

        for sid, last_ts in per_sensor.items():
            if not self.running:
                return
            name = sensors.get(sid, {}).get('name', sid)
            lag_min = (collector_hw - last_ts).total_seconds() / 60
            key = str(sid)

            state = self._sensor_state.get(key)

            if state is None:
                if lag_min <= stale_after_min:
                    continue  # keeping up with the pack - nothing to do
                # First time this sensor is seen behind.  Record the gap
                # now; once it resumes there is no way to find it again.
                self._sensor_state[key] = {
                    'gap_from': last_ts.isoformat(),
                    'stale_since': now,
                }
                logger.info(
                    "Sensor '%s' has fallen behind (no data since %s, %.1f h); "
                    "recorded its gap to back-fill when it returns", name,
                    last_ts.strftime('%Y-%m-%dT%X%z'), lag_min / 60)
                continue

            # A gap is on file for this sensor.  From here the current lag
            # is irrelevant - the sensor may already look current again
            # while the hole behind it is still unfilled.
            gap_from = datetime.datetime.fromisoformat(state['gap_from'])

            if not self._sensor_has_new_data(sid, gap_from, currenttime):
                if state.pop('returned_at', None):
                    logger.info("Sensor '%s' stopped reporting again before "
                                "its back-fill ran; rearming", name)
                else:
                    logger.info(
                        "Sensor '%s' still has no data since %s; nothing to "
                        "back-fill yet", name,
                        gap_from.strftime('%Y-%m-%dT%X%z'))
                continue

            if 'returned_at' not in state:
                state['returned_at'] = now
                logger.info(
                    "Sensor '%s' is reporting again (gap since %s); waiting %s "
                    "for its gateway to finish uploading before back-filling",
                    name, gap_from.strftime('%Y-%m-%dT%X%z'), grace_str)
                continue

            waited = now - float(state['returned_at'])
            # Small tolerance: probes happen once per deep scan, so an exact
            # comparison would routinely miss by a second and push the
            # back-fill out by a whole extra scan interval.
            if waited < grace_sec - 60:
                logger.info(
                    "Sensor '%s' back-fill pending (%.0f of %.0f min settle "
                    "time elapsed)", name, waited / 60, grace_sec / 60)
                continue

            logger.info("Sensor '%s' settled for %.0f min; back-filling from %s",
                        name, waited / 60, gap_from.strftime('%Y-%m-%dT%X%z'))
            if self._backfill_sensor(sid, gap_from, currenttime, sensors,
                                     measurement_name):
                self._sensor_state.pop(key, None)
                logger.info("Sensor '%s' back-fill complete", name)
            else:
                logger.warning(
                    "Sensor '%s' back-fill incomplete; will retry on the next "
                    "deep scan", name)

    def _sensor_has_new_data(self, sid, last_ts, currenttime):
        """Cheap probe: does the cloud hold anything newer for this sensor?

        The sensors endpoint carries no last-seen timestamp (``active``
        and ``rssi`` stay stale on a dead sensor), so presence has to be
        established with a one-sample query.  Starts just after the stored
        point so an inclusive range cannot re-report it as new.
        """
        probe_start = last_ts + datetime.timedelta(seconds=1)
        if probe_start >= currenttime:
            return False
        try:
            self._touch()
            samples = self.api.get_samples(
                datetime.date.strftime(probe_start, '%Y-%m-%dT%X%z'),
                datetime.date.strftime(currenttime, '%Y-%m-%dT%X%z'),
                measures=['temperature'], limit=1, sensors=[str(sid)])
            return int(samples.get('total_samples', 0)) > 0
        except Exception as e:
            logger.warning("Could not probe sensor %s for new data: %s", sid, e)
            return False

    def _backfill_sensor(self, sid, last_ts, currenttime, sensors,
                         measurement_name):
        """Fetch one sensor's whole gap. Returns True if every window landed."""
        start = last_ts - datetime.timedelta(hours=1)
        max_backfill_str = self.config['daemon']['max_backfill']
        if max_backfill_str and max_backfill_str != '0':
            earliest = currenttime - datetime.timedelta(
                minutes=parse_backlog(max_backfill_str))
            if start < earliest:
                logger.warning(
                    "Capping sensor back-fill start from %s to %s "
                    "(max_backfill=%s)", start.strftime('%Y-%m-%dT%X%z'),
                    earliest.strftime('%Y-%m-%dT%X%z'), max_backfill_str)
                start = earliest
        timelist = build_timelist(start, currenttime, self.args.timestep)
        iterations = len(timelist)
        logger.info("Sensor back-fill: %d iteration(s) from %s",
                    iterations, start.strftime('%Y-%m-%dT%X%z'))
        ok = True
        for i, window in enumerate(timelist, 1):
            if not self.running:
                return False
            if not self._fetch_and_write_window(
                    window, i, iterations, sensors, measurement_name,
                    sensor_filter=[str(sid)]):
                ok = False
        return ok

    def _record_failed_window(self, window):
        """Remember a window that could not be fetched, for a later retry."""
        entry = list(window)
        if entry not in self._failed_windows:
            self._failed_windows.append(entry)
            logger.error("Recorded failed window %s -> %s for retry "
                         "(%d pending)", entry[0], entry[1],
                         len(self._failed_windows))
            self._save_state()

    def _retry_failed_windows(self, sensors, measurement_name):
        """Re-drive previously failed windows; drop them once they land."""
        if not self._failed_windows:
            return
        pending = list(self._failed_windows)
        logger.info("Retrying %d previously failed window(s)", len(pending))
        for i, window in enumerate(pending, 1):
            if not self.running:
                return
            if self._fetch_and_write_window(
                    window, i, len(pending), sensors, measurement_name):
                try:
                    self._failed_windows.remove(window)
                except ValueError:
                    pass
        self._save_state()

    def _compute_oneshot_window(self, mytz, currenttime):
        """Compute time window for one-shot mode (backward compatible)."""
        if self.args.starttime:
            starttime = datetime.datetime.strptime(
                self.args.starttime, '%Y-%m-%dT%X%z')
        else:
            backlog = parse_backlog(self.args.backlog)
            starttime = currenttime - datetime.timedelta(minutes=backlog)

        if self.args.stoptime:
            stoptime = datetime.datetime.strptime(
                self.args.stoptime, '%Y-%m-%dT%X%z')
        else:
            stoptime = currenttime

        return starttime, stoptime

    def _fetch_and_write_window(self, window, iteration, iterations,
                                sensors, measurement_name, sensor_filter=None):
        """Fetch samples for one time window and write to backend.

        Returns True if the window was fetched and written, False if it
        was abandoned after MAXRETRY attempts (daemon mode).  The caller
        must not treat a False as progress - see _record_failed_window.
        """
        if sensor_filter is None:
            sensor_filter = self.args.sensorlist
        retrycount = 0
        while True:
            try:
                self._touch()
                logger.info("Iteration %d/%d (%s -> %s)",
                            iteration, iterations, window[0], window[1])

                # The SensorPush API defaults to ~10 samples per sensor
                # when no limit is specified.  In daemon mode we always
                # want all available data, so use a high limit.
                qlimit = self.args.qlimit
                if qlimit == 0:
                    qlimit = 10000

                samples = self.api.get_samples(
                    window[0], window[1],
                    measures=MEASURES,
                    limit=qlimit,
                    sensors=sensor_filter
                )

                truncated = samples['truncated']
                numsamples = samples['total_samples']
                numsensors = samples['total_sensors']

                logger.info("Request truncated: %s", truncated)
                if truncated:
                    logger.warning(
                        "Response truncated - consider reducing timestep")
                logger.info("Samples: %d, Sensors: %d",
                            numsamples, numsensors)

                records = process_samples(
                    samples, sensors, measurement_name,
                    self.config['my_altitude'], self.args.noconvert)

                if self.args.dryrun:
                    self._log_dryrun(records)
                    written = True
                else:
                    written = self._safe_write(records)

                # Delay between iterations
                if iterations > 1 and iteration < iterations:
                    logger.info("Sleeping for %d seconds", self.args.delay)
                    if self.args.daemon:
                        self._interruptible_sleep(self.args.delay)
                    else:
                        time.sleep(self.args.delay)
                return written

            except Exception as e:
                retrycount += 1
                logger.error("Error in iteration %d/%d (%s -> %s): %s",
                             iteration, iterations, window[0], window[1], e)

                if retrycount >= MAXRETRY:
                    if self.args.daemon:
                        # Report the window, not just the index - the index
                        # is meaningless once the cycle is over, and without
                        # the bounds the lost range cannot be found again.
                        logger.error(
                            "Max retries (%d) reached for window %s -> %s, "
                            "abandoning this attempt", MAXRETRY,
                            window[0], window[1])
                        return False
                    else:
                        logger.error(
                            "Max retries (%d) reached, stopping!",
                            MAXRETRY)
                        raise
                logger.info("Retrying in %ds (attempt %d/%d)",
                            RETRYWAIT, retrycount, MAXRETRY)
                self._interruptible_sleep(RETRYWAIT)

    def _safe_write(self, records):
        """Write records to all backends with retry and reconnection.

        Returns True if at least one backend accepted the batch.  In
        daemon mode a total failure is reported rather than raised, so
        the caller can decline to advance the high-water mark.
        """
        all_failed = True
        for writer in self.writers:
            if not self.running:
                return not all_failed

            # If never connected or previously marked disconnected, try to connect
            if not writer.connected:
                try:
                    logger.info("Attempting to (re)connect %s...",
                                type(writer).__name__)
                    writer.reconnect()
                    logger.info("Reconnected %s successfully",
                                type(writer).__name__)
                except Exception as e:
                    logger.warning("Reconnect %s failed: %s",
                                   type(writer).__name__, e)
                    continue  # skip this backend for this cycle

            for attempt, delay in enumerate([5, 10], 1):
                if not self.running:
                    return not all_failed
                try:
                    logger.debug("Writing %d records to %s (attempt %d)...",
                                 len(records), type(writer).__name__, attempt)
                    t0 = time.time()
                    writer.write(records)
                    logger.info("Write to %s OK (%d records, %.1fs)",
                                type(writer).__name__, len(records),
                                time.time() - t0)
                    writer._consecutive_failures = 0
                    all_failed = False
                    break
                except Exception as e:
                    logger.error("Write to %s failed (attempt %d/2, %.1fs): %s",
                                 type(writer).__name__, attempt,
                                 time.time() - t0, e)
                    if attempt < 2:
                        self._interruptible_sleep(delay)
            else:
                writer._consecutive_failures += 1
                logger.error("Failed to write to %s after 2 attempts "
                             "(%d consecutive), skipping batch",
                             type(writer).__name__,
                             writer._consecutive_failures)
                # After repeated failures, force reconnection on next cycle
                if writer._consecutive_failures >= 3:
                    logger.warning("Marking %s for reconnection after %d "
                                   "consecutive failures",
                                   type(writer).__name__,
                                   writer._consecutive_failures)
                    writer._connected = False

        if all_failed and not self.args.daemon:
            raise ConnectionError(
                "Failed to write to any backend after 3 attempts")
        return not all_failed

    def _log_dryrun(self, records):
        """Log records in dryrun mode."""
        logger.info("--- Data that would have been written ---")
        if self.args.verbose:
            for r in records:
                logger.info("%s", r)
        else:
            for r in records[:5]:
                logger.info("%s", r)
            if len(records) > 5:
                logger.info("... and %d more records", len(records) - 5)
        logger.info("--- End of dryrun data ---")

    def run_once(self):
        """Single collection cycle (backward compatible one-shot mode)."""
        self._connect_writers()
        try:
            self._collect_cycle()
        finally:
            for w in self.writers:
                try:
                    w.close()
                except Exception as e:
                    logger.warning("Error closing %s: %s",
                                   type(w).__name__, e)


# -----------------------------------------------------------------------------
# Display helpers (for --listsensors / --listgateways)
# -----------------------------------------------------------------------------
def display_gateways(gateways):
    for gid in gateways.keys():
        gwname = gateways[gid]["name"]
        print(f'---------------{gwname}---------------')
        print(f'Last alert               : {gateways[gid]["last_alert"]}')
        print(f'Last seen                : {gateways[gid]["last_seen"]}')
        print(f'Message                  : {gateways[gid]["message"]}')
        print(f'Paired                   : {gateways[gid]["paired"]}')
        print(f'Version                  : {gateways[gid]["version"]}')
        print('------------------------------------------------------------')
        print('')


def display_sensors(sensors):
    for sid in sensors.keys():
        sensorname = sensors[sid]["name"]
        print(f'---------------{sensorname}---------------')
        for key in sensors[sid].keys():
            print(f'{key}: {sensors[sid][key]}')
        try:
            float(sensors[sid]["battery_voltage"])
        except (KeyError, TypeError):
            print(f'Failed to get battery_voltage for {sensors[sid]["name"]}')
        try:
            float(sensors[sid]["rssi"])
        except (KeyError, TypeError):
            print(f'Failed to get rssi for {sensors[sid]["name"]}')
        print('------------------------------------------------------------')
        print('')


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    args = parse_args()

    # Setup logging
    setup_logging(level=args.loglevel, log_file=args.logfile)

    # Generate config and exit if requested
    if args.generate_config is not None:
        if args.generate_config:
            path = args.generate_config
        elif args.config:
            path = args.config
        else:
            path = f'{Path.home()}/.sensorpushd.conf'
        if Path(path).is_file():
            print(f'Config file already exists at {path} — not overwriting.')
            return 1
        create_default_config(path)
        print(f'Config template written to {path}')
        return 0

    # Determine config file
    if args.config:
        config_path = args.config
    else:
        homedir = str(Path.home())
        config_path = f'{homedir}/.sensorpushd.conf'
        # Fall back to legacy configs if new one doesn't exist
        if not Path(config_path).is_file():
            for legacy in [f'{homedir}/.sensorpush_vm.conf',
                           f'{homedir}/.sensorpush.conf']:
                if Path(legacy).is_file():
                    config_path = legacy
                    logger.info("Using legacy config: %s", config_path)
                    break

    # Load config
    config = load_config(config_path)

    # Force IPv4 once for the whole process, before any connection is made.
    if config['force_ipv4']:
        if enable_force_ipv4():
            logger.info("Forcing IPv4 for outbound connections")

    # Determine backend(s)
    if args.backend:
        backend_names = args.backend  # list from nargs='+'
    else:
        backend_names = [b.strip() for b in config['backend'].split(',')]

    # Only silence certificate warnings if a backend actually opts out of
    # verification, so a surprise on a verified connection still shows up.
    if any(config.get(name) and not config[name].get('verify_ssl', True)
           for name in ('influxdb2', 'influxdb3', 'victoriametrics')):
        suppress_insecure_warnings()

    # Handle list-only commands
    if args.listsensors or args.listgateways:
        api = SensorPushAPI(config['login'], config['password'],
                            verify_ssl=VERIFY_SSL)
        api.authenticate()
        if args.listgateways:
            display_gateways(api.get_gateways())
        if args.listsensors:
            display_sensors(api.get_sensors())
        return 0

    # Create API client and writers
    api = SensorPushAPI(config['login'], config['password'],
                        verify_ssl=VERIFY_SSL)
    logger.info("Active backend(s): %s", ', '.join(backend_names))
    writers = create_writers(backend_names, config)

    # Run
    daemon = SensorPushDaemon(api, writers, config, args)
    # Route the API client's retry sleeps through the daemon so they stay
    # interruptible and keep the watchdog heartbeat alive.
    api.sleep = daemon._interruptible_sleep

    if args.daemon:
        return daemon.run()

    daemon.run_once()
    return 0


if __name__ == '__main__':
    sys.exit(main())
