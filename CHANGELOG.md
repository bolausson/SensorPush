# Changelog

## Unreleased

### Unchanged on purpose

- `sensor_id` is stored exactly as before, `str(float(id))`, byte for byte on InfluxDB 2,
  InfluxDB 3 and VictoriaMetrics (see [issue #9](https://github.com/bolausson/SensorPush/issues/9)
  and the README section "Stored sensor ids"). Without a new config key, nothing that is
  written changes.

### Fixed

- **VictoriaMetrics gap detection never found a gap.** The newest stored point was read
  from the timestamp of an instant `last_over_time()` query, which is the query's
  evaluation time ("now"). Every series therefore looked current: after a daemon outage
  only the 10-minute poll backlog was fetched, and a sensor that went quiet was never
  noticed. The lookup now uses `tlast_over_time()` and reads the sample time from the
  value. Needs VictoriaMetrics v1.52 or later.
- **A failed gap lookup no longer counts as "no data".** Every backend turned any query
  error into "no data" (logging a warning per sensor), so a timeout during the first gap
  check after a restart dropped the whole outage. The same happened when the gap check
  skipped a backend whose last write had just failed. Lookups now raise. If a backend
  cannot be asked, the daemon resumes from the end of the last collection known to have
  completed (`collected_until` in `state.json`; on the first start after upgrading it is
  taken once from the old `last-success` marker) and logs a WARNING. A partial failure
  can no longer drag the window back to the last point of a long-dead sensor.
- **InfluxDB 3 gap lookups** search the last day, then the last 2 and 30 days, before
  falling back to an unbounded query. Sensors that reported within about two days are
  therefore found without exceeding InfluxDB 3 Core's default query file limit (432
  files, about three days of data); a sensor silent for longer may still need a query
  that exceeds it, and then sits out the gap check with a WARNING. A missing table or
  database counts as "no data".
- **`--dryrun` in daemon mode** no longer writes the `last-success` marker or
  `state.json`; it used to clear pending back-fills after a dry back-fill.
- **The freshness marker** (`last-success`) is refreshed after every window that lands,
  so a back-fill lasting hours no longer trips the freshness alert while data is flowing.
- **`--sensorlist`** now also limits the gap check and per-sensor back-fills. Excluded
  sensors with old data used to be tracked as stale and back-filled anyway.
- **Different `MEASUREMENT_NAME`s per backend.** All backends are written with the first
  backend's name, but each backend's gap check queried its own name, found nothing and
  silently stopped back-filling there. The gap check now queries the name that is
  written, and the daemon warns at startup.
- **`migrate_influx2vm.py`** copies `sensor_uid` as a label (it would have become a bogus
  metric), can read the unified config (`--config`), no longer appends the port to a URL
  that already has one, and its help shows the relative-time form that works
  (`--start=-30d`).
- **Generated config template** keeps its comments (they were silently dropped) and uses
  the documented upper-case keys. Its settings are unchanged.
- A non-numeric sensor id is stored unchanged and reported once, instead of failing every
  batch.

### Added

- **`SENSOR_UID`** (per backend, default off): also store the full SensorPush id as the
  tag `sensor_uid`. Read the README section "Storing the full sensor id" before enabling
  it on a database that already holds data.
- **`-l`** shows the `sensor_id` (and, if enabled, `sensor_uid`) each sensor is stored
  under; the daemon logs the mapping once at startup.
- An ERROR if two current sensors would be stored under the same `sensor_id` (not a
  practical risk; nothing is changed), and a WARNING on Python builds whose float repr is
  not "short".
- `sensorpush_csv-import.py --float-id`.
- An offline test suite: `python3 -m unittest discover -s tests`.

### Upgrade notes

- **VictoriaMetrics:** the first gap check after upgrading sees VictoriaMetrics' real
  newest point for the first time. If it had fallen behind - for example because it was
  unreachable while another backend kept working - the daemon back-fills that time once,
  writing it to all backends. A long gap can take a while; collection continues
  normally afterwards.
- **Read access:** gap detection asks each backend for its newest point, so the token
  needs read permission. Without it, the lookups fail; the daemon now resumes from the
  last completed collection instead of treating the database as empty, and logs a
  WARNING on every deep scan.
- **Dashboards:** queries that select or group by `sensor_name` are unaffected. The
  shipped VictoriaMetrics dashboard now combines series by `sensor_name`
  (`avg by (sensor_name)`); re-import it to pick that up.
