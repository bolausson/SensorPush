# Migration Guide: InfluxDB to VictoriaMetrics

> **Historical.** This guide compares two deprecated scripts, `sensorpush2.py` and
> `sensorpush_vm.py`. For new setups use `sensorpushd.py`, which writes InfluxDB 2,
> InfluxDB 3 and VictoriaMetrics, alone or together; see the [README](README.md). To copy
> existing InfluxDB data into VictoriaMetrics, use `migrate_influx2vm.py`.

## Overview

This guide explains the differences between `sensorpush2.py` (InfluxDB) and `sensorpush_vm.py` (VictoriaMetrics).

## Key Changes

### 1. Dependencies

**Before (sensorpush2.py):**
```python
from influxdb_client import Point, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
```

**After (sensorpush_vm.py):**
```python
# No InfluxDB dependencies - only uses standard requests library
```

### 2. Configuration File

**Before:** `~/.sensorpush.conf`
```ini
[INFLUXDBCONF]
MEASUREMENT_NAME = SensorPush
IFDB_URL = http://influxdb
IFDB_PORT = 8086
IFDB_TOKEN = your_token_here
IFDB_ORG = your_org
IFDB_BUCKET = sensorpush
IFDB_VERIFY_SSL = False
```

**After:** `~/.sensorpush_vm.conf`
```ini
[VICTORIAMETRICSCONF]
MEASUREMENT_NAME = SensorPush
VM_URL = http://localhost:8428
VM_VERIFY_SSL = False
```

### 3. Data Writing

**Before (InfluxDB):**
```python
# Initialize client
ifdbc = InfluxDBClient(url=f'{IFDB_URL}:{IFDB_PORT}', 
                       token=IFDB_TOKEN, 
                       org=IFDB_ORG, 
                       verify_ssl=IFDB_VERIFY_SSL)
ifdbc_write = ifdbc.write_api(write_options=SYNCHRONOUS)

# Write data
ifdbc_write.write(bucket=IFDB_BUCKET, org=IFDB_ORG, record=measurement)
```

**After (VictoriaMetrics):**
```python
# One JSON object per metric and point, in VictoriaMetrics' native import format:
# {"metric": {"__name__": "SensorPush_temperature", "sensor_id": ..., "sensor_name": ...},
#  "values": [21.5], "timestamps": [1767225600000]}
data = '\n'.join(json_lines)
requests.post(f'{VM_URL}/api/v1/import', data=data.encode('utf-8'),
              headers={'Content-Type': 'application/json'}, verify=VM_VERIFY_SSL)
```

## What Was Removed

1. ✅ **InfluxDB client library** - No longer needed
2. ✅ **Token authentication** - VictoriaMetrics' import endpoint doesn't require auth by default
3. ✅ **Organization/Bucket concepts** - VictoriaMetrics uses a simpler model
4. ✅ **Port configuration** - URL includes port now

## What Was Added

1. ✅ **JSON import converter** - each field becomes its own `<measurement>_<field>` metric
2. ✅ **Direct HTTP POST** - Simple requests.post() to `/api/v1/import` instead of a client library

## Data Format Compatibility

Measurement, tag and field names are the same, but **the value of `sensor_id` is not**:
`sensorpush2.py` (like `sensorpushd.py`) stores `str(float(id))`, e.g. `123456.78901234567`,
while `sensorpush_vm.py` stores the full SensorPush id, e.g. `123456.7890123456789012`. The
same sensor therefore has a different `sensor_id` in the two databases. Queries that select
by `sensor_name` work on both; queries that filter on `sensor_id` need the right form, or
both (see "Stored sensor ids" in the README).

### Measurements
- `SensorPush` - Main sensor readings
- `SensorPush_V` - Battery voltage and RSSI

### Tags
- `sensor_id` - Sensor identifier (stored form differs between the scripts, see above)
- `sensor_name` - Human-readable sensor name

### Fields
- `temperature` - Temperature (°C or °F)
- `humidity` - Relative humidity (%)
- `pressure` - Barometric pressure (mBar or inHg)
- `dewpoint` - Dew point temperature
- `vpd` - Vapor Pressure Deficit
- `abs_humidity` - Absolute humidity (g/m³)
- `altitude` - Altitude (meters or feet)
- `distance` - Distance measurement
- `voltage` - Battery voltage
- `rssi` - Signal strength

## Querying Data

### InfluxDB (Flux)
```flux
from(bucket: "sensorpush")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "SensorPush")
  |> filter(fn: (r) => r._field == "temperature")
```

### VictoriaMetrics (PromQL/MetricsQL)
```promql
SensorPush_temperature{sensor_name="Living Room"}
```

Or using InfluxDB-compatible query API:
```sql
SELECT temperature FROM SensorPush WHERE sensor_name='Living Room' AND time > now() - 1h
```

## Installation Steps

1. **Install VictoriaMetrics** (if not already running):
```bash
# Single-node version
docker run -d -p 8428:8428 \
  -v victoria-metrics-data:/victoria-metrics-data \
  victoriametrics/victoria-metrics:latest
```

2. **Copy and configure the new script**:
```bash
cp sensorpush2.py sensorpush_vm.py  # Already done
chmod +x sensorpush_vm.py
./sensorpush_vm.py  # Creates config template
nano ~/.sensorpush_vm.conf  # Edit configuration
```

3. **Test the script**:
```bash
./sensorpush_vm.py -b 1h -x  # Dry run
./sensorpush_vm.py -b 1h     # Real run
```

4. **Verify data in VictoriaMetrics**:
```bash
# List all metrics
curl http://localhost:8428/api/v1/label/__name__/values

# Query specific metric
curl 'http://localhost:8428/api/v1/query?query=SensorPush_temperature'
```

## Grafana Configuration

VictoriaMetrics is compatible with Prometheus data source in Grafana:

1. Add new data source → Prometheus
2. URL: `http://victoriametrics:8428`
3. Save & Test

Your existing dashboards should work with minimal changes if you update the queries from Flux to PromQL.

## Performance Comparison

| Feature | InfluxDB 2.x | VictoriaMetrics |
|---------|--------------|-----------------|
| Write Speed | Fast | Faster (10x+ in some cases) |
| Storage | Good compression | Better compression |
| Memory Usage | Higher | Lower |
| Query Language | Flux | PromQL/MetricsQL + InfluxQL |
| Clustering | Enterprise only | Built-in (free) |
| Dependencies | Many | Minimal |

## Troubleshooting

### Script fails with "Connection refused"
- Check VictoriaMetrics is running: `curl http://localhost:8428/health`
- Verify VM_URL in config matches your setup

### No data appears in VictoriaMetrics
- Run with `-x` flag to see what would be written
- Check VictoriaMetrics logs for errors
- Verify timestamp format is correct

### Import errors (HTTP 400)
- Check for special characters in sensor names
- Verify field values are valid numbers
- Use dry-run mode to inspect the generated JSON lines

## Rollback Plan

If you need to go back to InfluxDB:

1. Keep `sensorpush2.py` and `~/.sensorpush.conf` unchanged
2. Run both scripts in parallel during transition
3. Compare data in both databases
4. Switch Grafana data source back to InfluxDB if needed

If you also copy the InfluxDB history into VictoriaMetrics with `migrate_influx2vm.py`
while `sensorpush_vm.py` is running, VictoriaMetrics ends up with both `sensor_id` forms
for the same sensor: the migrated history carries the shortened form, the new data the full
id. That is harmless for panels keyed on `sensor_name`, but queries filtering on
`sensor_id` must match both.

## Grafana Dashboard

An example Grafana dashboard for VictoriaMetrics is included:

- `SensorPush-GrafanaDashboard-vm.json` - Import this into Grafana with a Prometheus data source pointing to VictoriaMetrics

## Support

For issues specific to:
- **SensorPush API**: Check SensorPush documentation
- **VictoriaMetrics**: https://docs.victoriametrics.com/
- **This script**: See the main README.md file

