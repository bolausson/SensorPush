# SensorPush
[SensorPush](http://www.sensorpush.com/) recently introduced a [API](http://www.sensorpush.com/api/docs) to query the temperature and humidity samples recorded by their smart sensors via G1 WIFI GATEWAY - As long as the API is in beta stage, you have to contact support to get access!

This Python 3 tool can query the API and save the temperature and humidity time series to a time series database so it can easily be plotted with Grafana.

If you don't have an G1 WIFI Gateway and still want to plot your temperature, you can use another little tool I wrote to feed the CSV file which you can export via the Android App to InfluxDB.

![Grafana](https://github.com/bolausson/SensorPush/blob/master/SensorPush-Grafana-InfluxDB.png?raw=true)

## Available Scripts

There are three versions of the script for different database backends:

| Script | Database | Description |
|--------|----------|-------------|
| `sensorpush.py` | InfluxDB 1.x | Original script for InfluxDB 1.x |
| `sensorpush2.py` | InfluxDB 2.x | Updated script for InfluxDB 2.x with Flux query language |
| `sensorpush_vm.py` | VictoriaMetrics | Script for VictoriaMetrics (Prometheus-compatible) |

Each script has its own configuration file:
- `~/.sensorpush.conf` - for InfluxDB 1.x
- `~/.sensorpush2.conf` - for InfluxDB 2.x
- `~/.sensorpush_vm.conf` - for VictoriaMetrics

## Grafana Dashboards

Example Grafana dashboards are included:
- `SensorPush-GrafanaDashboard.json` - Dashboard for InfluxDB
- `SensorPush-GrafanaDashboard-vm.json` - Dashboard for VictoriaMetrics (Prometheus data source)


## API query
```
# sensorpush.py --help
usage: sensorpush.py [-h] [-s STARTTIME] [-p STOPTIME] [-b BACKLOG]
                     [-t TIMESTEP] [-q QLIMIT] [-d DELAY] [-l] [-g]
                     [-i SENSORLIST [SENSORLIST ...]] [-n] [-x]

Queries SensorPus API and stores the temp and humidity readings in InfluxDB

optional arguments:
  -h, --help            show this help message and exit
  -s STARTTIME, --start STARTTIME
                        start query at time (e.g. "2019-07-25T00:10:41+0200")
  -p STOPTIME, --stop STOPTIME
                        Stop query at time (e.g. "2019-07-26T00:10:41+0200")
  -b BACKLOG, --backlog BACKLOG
                        Historical data to fetch (default 1 day) - time can be
                        specified in the format <number>[m|h|d|w|M|Y]. E.g.:
                        10 Minutes = 10m, 1 day = 1d, 1 month = 1M
  -t TIMESTEP, --timestep TIMESTEP
                        Time slice per query (in minutes) to fetch (default
                        720 minutes [12 h])
  -q QLIMIT, --querylimit QLIMIT
                        Number of samples to return per sensor (default unset
                        = API default limimt [10])
  -d DELAY, --delay DELAY
                        Delay in seconds between queries
  -l, --listsensors     Show a list of sensors and exit
  -g, --listgateways    Show a list of gateways and exit
  -i SENSORLIST [SENSORLIST ...], --sensorlist SENSORLIST [SENSORLIST ...]
                        List of sensor IDs to query
  -n, --noconvert       Do not convert °F to °C, inHG to mBar, kPa to mBar and feet
                        to meters
  -x, --dryrun          Do not write anything to the database, just print what
                        would have been written
  -v, --verbose         Show full output in dryrun mode (do not truncate)
```

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
