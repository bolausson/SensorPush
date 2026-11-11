# Changelog

## Unreleased

### Unchanged on purpose

- `sensor_id` is stored exactly as before, `str(float(id))`, byte for byte on InfluxDB 2,
  InfluxDB 3 and VictoriaMetrics (see [issue #9](https://github.com/bolausson/SensorPush/issues/9)
  and the README section "Stored sensor ids"). The format and values of what is written do
  not change; gap re-checks (below) only write the same points as the polls, later.
- Every sample the API serves is stored, as before, including the live copies it serves
  for the newest minutes (see [issue #9](https://github.com/bolausson/SensorPush/issues/9)
  and the README section "Live and final samples").

### Fixed

- **Most samples were never stored.** The SensorPush API hands out each sensor's samples
  in chunks of about 15 minutes and selects a chunk by its *start* time: a query from T
  returns only chunks that started at or after T, even when an earlier chunk holds samples
  observed after T. The daemon's polls reached back only `POLL_BACKLOG` (default 10
  minutes), so most chunks were never fetched - on one installation about three quarters
  of all samples have been missing since the daemon replaced the cron scripts - and
  nothing noticed, because the collector as a whole always looked current. (Comparing
  point counts with the API shows less, because the database also holds live copies; see
  the README section "Live and final samples".) `POLL_BACKLOG` now defaults to `1h`, and
  shorter settings (including the `10m` that generated configs spell out) are raised to
  1h with a warning. The same floor applies to one-shot `-b`.
- **Outages were only partly recovered.** After an outage the cloud fills in the past
  late: the gateway uploads what its sensors recorded meanwhile, a small backlog usually
  within an hour, HT1 history sometimes hours or weeks later. The daemon read each gap
  once and took a read that worked for a complete one. So it lost most of any silence of
  1 to 6 hours (only a sensor 6 hours behind the others counted as missing); most of an
  outage that hit every sensor at once - both gateways, the internet or the power down -
  because the one catch-up read came before the upload; and whatever reached the cloud
  more than about an hour after the sensor returned. The daemon now notes every gap in a
  sensor's data (a silence of more than `RECHECK_MIN_GAP`, default 20 minutes; in HT1 data
  it did not see live, 90 minutes) and reads it again 1h, 6h, 1d, 3d and 7d after it
  closed (`RECHECK_GAPS`), writing what the cloud holds by then. It also reads once more
  what a poll window did not reach (the API leaves out up to 15 minutes after a window's
  start): after a silence or a daemon pause of 45 minutes or more, and with polls more
  than 30 minutes apart. With an `INTERVAL` of 45 to 59 minutes the polls lost samples
  that way, at 59 minutes about 1,100 a day on 18 sensors. Without gaps the re-checks
  cost no API calls at an `INTERVAL` of 30 minutes or less, or of 60 or more (with the
  default `DEEP_SCAN_INTERVAL`); with polls more than 30 and less than 60 minutes apart
  they cost up to 35 calls a day on 18 sensors (the most at 40 minutes), and a daemon
  pause of 40 to 60 minutes one call at most. Re-reads are limited to 6 calls and about 2
  minutes per cycle, wait while any backend fails its writes, and one call serves every
  gap in its window. A call that failed is tried again after a pause. A window that keeps
  failing is read alone, one sensor per call, on a pause of its own, after the gaps read
  together, so it holds none of them up. The one exception is the first read alone of a
  window that failed twice: it comes in turn, and if a backend refuses its write, no
  further re-read is made in that cycle. However many windows are read alone, they cost
  at most one failed call per pause, about 27 a day at an `INTERVAL` of 5 minutes. On a
  gap's last re-read a failing window is kept and tried again; only a call that fails
  more than a day after the last re-read time gives it up. Windows read for the last
  time get every other read alone while other windows wait too, as the tries of their
  last day go by while they wait; a window whose last write a backend refused waits with
  the others, as it would most likely be refused again. The other windows read alone go
  one gap at a time, so a gap whose windows all fail - one that reaches beyond a
  bucket's retention, say - gets through them one window per pause, one per two pauses
  while a window that takes every other read is due. The gaps are kept in `state.json`
  (key `rechecks`; older releases ignore it), at most 40 per sensor and 1,000 in all.
  `RECHECK_GAPS = 0` restores the previous behaviour exactly, also in a `[DAEMON]`
  section of a legacy config file. Still lost: data that reaches the cloud more than 7
  days after a gap closed; data beyond a sensor's on-board storage (HT1 about 20 days,
  HTP.xw 30, HT.w 45); an HT1 silence of 20 to 90 minutes that the daemon did not see
  live; and history that ages out of an InfluxDB 2 bucket's retention before the
  re-reads get to it (after a 15-day outage of 18 sensors with a 14-day bucket, 91 % of
  what the bucket would still take is written at the default `-t 720`, less with a
  smaller `-t`). With a bucket that keeps less than the last re-read time plus the gap
  and an hour (with the default `RECHECK_GAPS`, any bucket of 7 days or less), set
  `RECHECK_GAPS` to times inside it.
  See the README section "Gap re-checks".
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

- **Recover the samples short polls missed.** The SensorPush cloud still has them (at
  least several months back). Back-fill from when you started the daemon with a one-shot
  run, e.g. `./sensorpushd.py -c /etc/sensorpushd.conf -s 2026-07-01T00:00:00+0200 -d 10`.
  It writes the same tags, so no new series appear. Samples already stored are overwritten
  with identical values and missing ones are added as new points; live copies stored
  earlier stay (README section "Live and final samples"), so a back-filled day holds more
  points than a re-read of it returns. One caveat: it stores every sample under the sensor's
  *current* name, so a sensor renamed within that period would get its earlier samples a
  second time, under the new name. Leave such a sensor out of the main run (list the
  others with `-i`) and back-fill it on its own from the rename onwards.
- **Bigger polls:** every poll now fetches about an hour of samples (about 60 per sensor
  instead of about 10). On InfluxDB 2 and 3, writing a point that is already stored
  replaces it with identical values, so this does no harm. The polls also catch the live
  copy of most readings, so with the default settings the database gets about twice as
  many points as a re-read returns; queries that count points or take derivatives of raw
  points need adjusting (README section "Live and final samples").
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
