# SensorPush
[SensorPush](http://www.sensorpush.com/) recently introduced a [API](http://www.sensorpush.com/api/docs) to query the temperature and humidity samples recorded by their smart sensors via G1 WIFI GATEWAY - As long as the API is in beta stage, you have to contact support to get access!

This Python 3 tool can query the API and save the temperature and humidity time series to a time series database so it can easily be plotted with Grafana.

If you don't have an G1 WIFI Gateway and still want to plot your temperature, you can use another little tool I wrote to feed the CSV file which you can export via the Android App to InfluxDB.

![Grafana](https://github.com/bolausson/SensorPush/blob/master/SensorPush-Grafana-InfluxDB.png?raw=true)

## sensorpushd.py - Unified Daemon (Recommended)

`sensorpushd.py` is the unified replacement for the individual scripts. It supports **InfluxDB 2**, **InfluxDB 3**, and **VictoriaMetrics** in a single tool — including writing to **multiple backends simultaneously**. It can run either as a one-shot command (for cron) or as a continuous daemon (managed by systemd).

### Installation

```bash
# Required for all backends:
pip install requests

# Only install what you need for your chosen backend:
pip install influxdb-client      # for InfluxDB 2
pip install influxdb3-python     # for InfluxDB 3
# VictoriaMetrics needs no extra packages
```

### Quick Start

```bash
# First run creates a config template at ~/.sensorpushd.conf
./sensorpushd.py

# Edit the config file with your SensorPush credentials and backend settings
nano ~/.sensorpushd.conf

# One-shot: fetch last day of data (backward compatible with cron)
./sensorpushd.py -b 1d --backend victoriametrics

# Write to multiple backends simultaneously
./sensorpushd.py -b 1d --backend victoriametrics influxdb3

# Daemon mode: continuous polling every 5 minutes
./sensorpushd.py --daemon --interval 300

# Dry run to preview data without writing
./sensorpushd.py -b 10m -x --backend victoriametrics

# List your sensors
./sensorpushd.py -l
```

### Migrating from the old scripts

Your existing config files work directly:

```bash
# Use existing InfluxDB 2 config
./sensorpushd.py -c ~/.sensorpush.conf -b 1d

# Use existing VictoriaMetrics config
./sensorpushd.py -c ~/.sensorpush_vm.conf -b 1d
```

### Configuration

The new unified config file (`~/.sensorpushd.conf`) supports all backends. Only configure the sections for the backend(s) you use. To write to multiple backends simultaneously, set `TYPE` to a comma-separated list:

```ini
[SENSORPUSHAPI]
LOGIN = your_email@example.com
PASSWD = your_password

[BACKEND]
TYPE = victoriametrics
# For multiple backends: TYPE = victoriametrics, influxdb3

[INFLUXDB2]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8086
TOKEN = your_token
ORG = your_org
BUCKET = sensorpush
VERIFY_SSL = False

[INFLUXDB3]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8181
DATABASE = sensorpush
TOKEN = your_token
VERIFY_SSL = False

[VICTORIAMETRICS]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8428
VERIFY_SSL = False

[DAEMON]
INTERVAL = 300
POLL_BACKLOG = 10m

[MISC]
MY_ALTITUDE = 0.0
FORCE_IPv4 = False
```

### Running as a systemd service

```bash
# Copy and edit the service file
sudo cp sensorpushd.service /etc/systemd/system/
sudo nano /etc/systemd/system/sensorpushd.service  # adjust paths

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now sensorpushd

# Check status and logs
sudo systemctl status sensorpushd
sudo journalctl -u sensorpushd -f
```

### Daemon features

- **Multi-backend**: Write to multiple databases simultaneously; each backend is independent — one failure does not block the others
- **Gap-filling**: If the API or database was unavailable, the daemon automatically detects the gap on the next successful cycle and back-fills the missing data
- **Graceful shutdown**: Responds to SIGTERM/SIGINT for clean shutdown
- **Error recovery**: Retries with exponential backoff on API or backend failures; never exits on transient errors
- **Token refresh**: Proactively re-authenticates before the OAuth token expires
- **Configurable logging**: Use `--log-level` and `--log-file` for production logging

### CLI reference

```
./sensorpushd.py --help
```

All arguments from the old scripts are preserved (`-s`, `-p`, `-b`, `-t`, `-q`, `-d`, `-l`, `-g`, `-i`, `-n`, `-x`, `-v`), plus new ones:

| Argument | Description |
|----------|-------------|
| `--backend` | One or more of: `influxdb2`, `influxdb3`, `victoriametrics` |
| `--daemon` | Run as a continuous daemon |
| `--interval` | Polling interval in seconds (default: 300) |
| `-c`/`--config` | Path to config file |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `--log-file` | Log to file instead of stderr |

## Legacy Scripts (Deprecated)

The individual scripts are still available but deprecated in favor of `sensorpushd.py`:

| Script | Database | Config File |
|--------|----------|-------------|
| `sensorpush.py` | InfluxDB 1.x | `~/.sensorpush.conf` |
| `sensorpush2.py` | InfluxDB 2.x | `~/.sensorpush.conf` |
| `sensorpush_vm.py` | VictoriaMetrics | `~/.sensorpush_vm.conf` |

## Grafana Dashboards

Example Grafana dashboards are included:
- `SensorPush-GrafanaDashboard.json` - Dashboard for InfluxDB
- `SensorPush-GrafanaDashboard-vm.json` - Dashboard for VictoriaMetrics (Prometheus data source)

## Migration from InfluxDB to VictoriaMetrics

If you have existing SensorPush data in InfluxDB and want to migrate to VictoriaMetrics, use the `migrate_influx2vm.py` script:

```
# migrate_influx2vm.py --help
usage: migrate_influx2vm.py [-h] [--ifdb-config IFDB_CONFIG] [--vm-config VM_CONFIG]
                            [--start START_TIME] [--end END_TIME] [--all]
                            [--batch-size BATCH_SIZE] [--chunk-days CHUNK_DAYS]
                            [--dry-run] [--verbose]

Migrate SensorPush data from InfluxDB 2.x to VictoriaMetrics

optional arguments:
  -h, --help            show this help message and exit
  --ifdb-config IFDB_CONFIG
                        Path to InfluxDB config file (default: ~/.sensorpush.conf)
  --vm-config VM_CONFIG
                        Path to VictoriaMetrics config file (default: ~/.sensorpush_vm.conf)
  --start START_TIME    Start time (ISO format or relative like -7d)
  --end END_TIME        End time (ISO format, default: now)
  --all                 Migrate all data from earliest available
  --batch-size BATCH_SIZE
                        Batch size for writes (default: 10000)
  --chunk-days CHUNK_DAYS
                        Days per chunk for large migrations (default: 7)
  --dry-run             Preview migration without writing data
  --verbose             Show detailed output
```

Example usage:
```bash
# Migrate all historical data
python migrate_influx2vm.py --all

# Migrate last 30 days
python migrate_influx2vm.py --start="2026-01-01T00:00:00Z"

# Preview migration without writing
python migrate_influx2vm.py --all --dry-run
```

## CSV import
```
# sensorpush_csv-import.py --help
usage: sensorpush_csv-import.py [-h] [-f CSVFILE] [-s SENSORNAME]
                                [-i SENSORID] [-d] [-c CHUNKS]

Reads a CSV file exported from the SensorPush Android App and stores the temp
and humidity readings in InfluxDB

optional arguments:
  -h, --help            show this help message and exit
  -f CSVFILE, --csvfile CSVFILE
                        CSV file exported from the SensorPush Android App
  -s SENSORNAME, --sensorname SENSORNAME
                        Sensor name
  -i SENSORID, --sensorid SENSORID
                        Sensor id
  -d, --dryrun          Do not write anything to InfluxDB - just print what
                        would have been written
  -c CHUNKS, --chunks CHUNKS
                        Write data in chunks to InfluxDB to not overload e.g.
                        a RaspberryPi
```
