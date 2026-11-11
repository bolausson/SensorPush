# SensorPush
[SensorPush](http://www.sensorpush.com/) recently introduced a [API](http://www.sensorpush.com/api/docs) to query the temperature and humidity samples recorded by their smart sensors via G1 WIFI GATEWAY - As long as the API is in beta stage, you have to contact support to get access!

This Python 3 tool can query the API and save the temperature and humidity time series to a time series database so it can easily be plotted with Grafana.

If you don't have an G1 WIFI Gateway and still want to plot your temperature, you can use another little tool I wrote to feed the CSV file which you can export via the Android App to InfluxDB.

![Grafana](https://github.com/bolausson/SensorPush/blob/master/SensorPush-Grafana-InfluxDB.png?raw=true)

## sensorpushd.py - Unified Daemon (Recommended)

`sensorpushd.py` is the unified replacement for the individual scripts. It supports **InfluxDB 2**, **InfluxDB 3**, and **VictoriaMetrics** in a single tool — including writing to **multiple backends simultaneously**. It can run either as a one-shot command (for cron) or as a continuous daemon (managed by systemd).

Changes and upgrade notes are listed in [CHANGELOG.md](CHANGELOG.md). The offline test suite runs with `python3 -m unittest discover -s tests`.

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
./sensorpushd.py -b 1h -x --backend victoriametrics

# List your sensors, including the sensor_id each one is stored under
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

Coming from `sensorpush_vm.py` (or `sensorpush.py`): those scripts stored the full
SensorPush id in `sensor_id`, `sensorpushd.py` stores the shortened form described in
[Stored sensor ids](#stored-sensor-ids). Each sensor therefore continues in a new series.
Panels that select or group by `sensor_name` are unaffected; queries that filter on
`sensor_id` need both forms to cover the whole history.

### Configuration

The new unified config file (`~/.sensorpushd.conf`) supports all backends. Only configure the sections for the backend(s) you use. To write to multiple backends simultaneously, set `TYPE` to a comma-separated list. All backends are written with the `MEASUREMENT_NAME` of the first backend in `TYPE` (the daemon warns at startup if the sections disagree):

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
# Optional, see "Storing the full sensor id" below. Default: False
SENSOR_UID = False

[INFLUXDB3]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8181
DATABASE = sensorpush
TOKEN = your_token
VERIFY_SSL = False
SENSOR_UID = False

[VICTORIAMETRICS]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8428
VERIFY_SSL = False
SENSOR_UID = False

[DAEMON]
INTERVAL = 300
# How far every poll reaches back; at least 1h (see "Daemon features")
POLL_BACKLOG = 1h
# When to re-read a gap after it closed; 0 = off (see "Gap re-checks")
RECHECK_GAPS = 1h, 6h, 1d, 3d, 7d
# The shortest silence of a sensor that counts as a gap
RECHECK_MIN_GAP = 20m

[MISC]
MY_ALTITUDE = 0.0
FORCE_IPv4 = False
```

### Stored sensor ids

SensorPush sensor ids look like `123456.7890123456789012`: the device id, a dot and about
16 more digits. The `sensor_id` tag stores `str(float(id))`, Python's shortest form of the
nearest floating-point number, which keeps 15-17 significant digits:

| SensorPush id (API and app) | stored `sensor_id` |
|---|---|
| `123456.7890123456789012` | `123456.78901234567` |
| `12345678.9012345678901` | `12345678.901234567` |

The part before the dot is always kept in full, and the mapping is fixed, so any id can be
turned into its stored value:

```bash
./sensorpushd.py -l                                          # every sensor, with its stored sensor_id
python3 -c 'print(float("123456.7890123456789012"))'         # 123456.78901234567
```

`sensor_id` does not change when you rename a sensor, so it is the key for one particular
device. `sensor_name` follows renames, and the same name can later be given to a
replacement sensor, so it is the key for a place.

This format is kept on purpose. Every existing series, every query or alert that filters on
`sensor_id`, and the daemon's own gap detection depend on it; storing a different value
would start new series and make gap detection blind to existing data (see
[issue #9](https://github.com/bolausson/SensorPush/issues/9)). To store the full id as
well, see [Storing the full sensor id](#storing-the-full-sensor-id-sensor_uid).

**Which script stored which form.** `sensorpush.py` and `sensorpush_vm.py` stored the
full id, `sensorpush2.py` and `sensorpushd.py` store the shortened one. If you switched
between them, a sensor's history is split across both values. To query all of it, match
both:

```sql
-- InfluxQL
SELECT mean("temperature") FROM "SensorPush"
  WHERE ("sensor_id" = '123456.78901234567' OR "sensor_id" = '123456.7890123456789012') AND $timeFilter
  GROUP BY time($__interval)
```
```flux
// Flux (use == ... or ==, not contains(), which is not pushed down to storage)
|> filter(fn: (r) => r.sensor_id == "123456.78901234567" or r.sensor_id == "123456.7890123456789012")
```
```sql
-- SQL (InfluxDB 3)
SELECT ... FROM "SensorPush" WHERE sensor_id IN ('123456.78901234567', '123456.7890123456789012')
```
```promql
# PromQL / MetricsQL - label regexes are anchored; escape the dots
avg by (sensor_name) (SensorPush_temperature{sensor_id=~"123456\\.78901234567|123456\\.7890123456789012"})
```

For a quick look, the device id alone also matches both forms, e.g. InfluxQL
`"sensor_id" =~ /^123456\./`. Avoid it in saved queries: it also matches any earlier id
the same device had if it was ever removed and registered again.

### Storing the full sensor id (SENSOR_UID)

Set `SENSOR_UID = True` in a backend's section (`[INFLUXDB2]`, `[INFLUXDB3]`,
`[VICTORIAMETRICS]`, or `[INFLUXDBCONF]` / `[VICTORIAMETRICSCONF]` in the legacy config
files) and every point written to that backend from then on also carries the full id as
the tag `sensor_uid`. It is off by default, and `sensor_id` keeps its value either way.

What enabling it changes:

- **New series.** Like renaming a sensor, every sensor continues in a new series from the
  switch on; points stored before it have no `sensor_uid`. Queries that filter or group
  by `sensor_id` or `sensor_name`, and the shipped dashboards, keep showing one line.
  Queries that keep every tag apart - InfluxQL `GROUP BY *`, Flux without `group()`,
  PromQL/MetricsQL without `sum by`/`avg by` - show the old and the new series side by side.
- **A one-time overlap.** After the restart that enables it, the daemon re-reads the
  last `POLL_BACKLOG` (one hour), or more if it was stopped for longer than that. Those points are then stored with and without
  `sensor_uid`: `mean()` is unaffected, `count()` and `sum()` double for that stretch.
  The same happens whenever time from before the switch is fetched again, e.g. a one-shot
  run with `-s`/`-p`, or a back-fill for a second backend that had fallen behind.
- **Nothing else.** Gap detection and back-fills still key on `sensor_id` and find old and
  new series alike.
- **Backend support.** Early InfluxDB 3 Core builds (2024-12 to 2025-03) rejected
  writes that add a new tag to an existing table, and InfluxDB 3 Enterprise databases
  (or InfluxDB Cloud buckets) with an explicit schema need the tag declared first. If a
  backend rejects the writes, its log shows write errors; set `SENSOR_UID = False` again.

Turning it off again splits the series once more, the same way. `migrate_influx2vm.py`
copies `sensor_uid` as a label.

### Live and final samples

For the newest minutes the SensorPush API serves most readings twice: first as a live
copy at an irregular second, and later as the sensor's own sample, on a fixed second of
the minute and usually within a minute of the copy (before or after it, depending on the
sensor). After about half an hour the API normally serves only the sensor's own samples,
so a later re-read or back-fill returns only those.

The daemon stores every sample the API serves. With the default settings a sensor
therefore has about twice as many points as a re-read returns. Data written by versions
with a 10-minute backlog contains live copies too, and so can data from the old scripts;
a back-fill does not remove them. This is on purpose: the live copies are the only data
for the newest minutes, and some readings are never served in any other form. No stored
field marks a live copy. In the API's answer an HT1's live copy names the gateway that
relayed it, while its own sample leaves `gateways` empty, but the daemon does not store
that field, and on HT.w and HTP.xw both carry it. In the database the best hint is the
timestamp: a sensor's own samples sit on one second of the minute (which can shift after
a gap), and live copies almost never do.

What this means for queries:

- Averages, minimum and maximum are practically unaffected, the newest value is more
  current, and the shipped dashboards need no change.
- `count()` counts both copies. To check completeness, count the minutes that have data
  rather than the points, and compare a sensor with itself, not with other models. HT.w
  and HTP.xw sensors have data in every minute. HT1 sensors seem to leave a reading out of
  their own record when neither temperature nor humidity has changed: polled live, most
  of mine have data in 40 to 55 minutes of an hour (the live copies fill part of the
  holes), and a re-read or back-fill returns fewer. On an HT1 a missing minute is not a
  lost reading, but a long silence points to missing data: in more than three months of
  complete record, no working HT1 of mine was silent for more than about an hour, and no
  HT.w or HTP.xw for more than about a minute.
- `derivative()`, `difference()` and similar functions on raw points spike, because a live
  copy and the neighbouring sample can be seconds apart and one reading step different.
  Aggregate first.

```sql
-- InfluxQL: minutes with data, per sensor
SELECT count("c") FROM (
  SELECT count("temperature") AS "c" FROM "SensorPush"
  WHERE $timeFilter GROUP BY time(1m), "sensor_name" fill(none)
) WHERE $timeFilter GROUP BY "sensor_name"

-- InfluxQL: rate of change of 5-minute means
SELECT derivative(mean("temperature"), 1m) FROM "SensorPush"
  WHERE $timeFilter GROUP BY time(5m), "sensor_name"

-- InfluxQL: silences longer than an hour, per sensor (one still going on is not listed)
SELECT "gap" FROM (
  SELECT elapsed("temperature", 1m) AS "gap" FROM "SensorPush"
  WHERE $timeFilter GROUP BY "sensor_name"
) WHERE "gap" > 60 GROUP BY "sensor_name"
```

### Gap re-checks

After an outage the cloud fills in the past late. When a gateway, the internet or the
power comes back, the gateway uploads what its sensors recorded meanwhile: a small
backlog usually within about an hour, the history of an HT1 sensor sometimes hours or
weeks later. One read right after the outage finds only part of it.

So the daemon notes every gap it sees in a sensor's data, a silence of more than
`RECHECK_MIN_GAP` (default 20 minutes, at least 15), and reads it again, with an hour on
either side, 1 hour, 6 hours, 1 day, 3 days and 7 days after the sensor reported again
(`RECHECK_GAPS`). It writes whatever the cloud holds by then: the same points the polls
write, with the same tags, fields and timestamps.

It also notes what a poll did not reach. A poll looks back an hour, and the API leaves
out up to 15 minutes after the start of a window in the newest hour (see "Complete
polls"). A sensor's samples right after the last one the daemon saw can lie in that
stretch, even when no gap shows: after the sensor was silent for about 45 minutes or
more, after the daemon paused that long, and with polls more than 30 minutes apart. The
daemon reads such a stretch once more at the first re-read time, an hour later, and
writes what it holds. Only if that read shows a gap, or could not read all of it, is the
stretch read on like a gap.

What it costs:

- No API calls while no sensor has a gap and `INTERVAL` is 30 minutes or less, or 60
  minutes or more with the default `DEEP_SCAN_INTERVAL` of 1h.
- With polls more than 30 and less than 60 minutes apart, the stretches a poll did not
  reach cost calls also without any gap. Measured on 18 sensors (12 HT1, 2 HTP.xw, 4
  HT.w) with the default `DEEP_SCAN_INTERVAL`: 4 to 13 calls a day at an `INTERVAL` of 35
  minutes and 26 to 35 at 40 (more when HT1 live copies are further apart), 16 at 45, 14
  at 50, 13 at 55 and 12 at 59. Below 45 minutes they find nothing on a normal day. From
  45 minutes on they fetch what the API left out after each poll's start: at 59 minutes
  about 1,100 samples a day on those 18 sensors, which are lost without them. With an
  `INTERVAL` of 60 minutes or more and a longer `DEEP_SCAN_INTERVAL` it is up to one call
  per poll (20 a day at 60 minutes with 6h).
- A pause of the daemon of 40 to 60 minutes costs one call at most.
- One call per re-read of a gap up to about 9 hours long (with the default `-t 720`), one
  more per further 11 hours. A call serves every gap that fits in its window, so after an
  outage of the whole installation one call covers all sensors, also when they come back
  minutes apart.
- At most 6 re-read calls and about 2 minutes per cycle (2 calls with the default
  `--delay 60`). Long gaps are read over several cycles while the polls go on.
- An HTP.xw or HT.w gap that a re-read finds complete (a sample every minute) is not
  read again, and a gap longer than a day whose first re-read found nothing is next read
  after 3 days (if `RECHECK_GAPS` has a read that late).
- After a failed re-read call, re-reads pause for one polling interval, longer after each
  further failure in a row, up to an hour; any call that works starts that over. They
  also wait while the polls fail, and while any backend fails its writes (checked before
  each call), so that every backend gets what they write.
- A window that fails twice in a row is then read alone: one sensor per call, so that
  one sensor's data cannot fail the others' call. The first of these reads decides for
  the others: if it works, they are read together again (once per window and re-read);
  if it fails, they are read alone. Apart from that first read, reads alone come after
  the gaps read together and hold up none of them. Reads alone pause on their own after
  a failure, with the same pauses: however many windows keep failing, they make at most
  one failed call per pause, about 27 a day at an `INTERVAL` of 5 minutes. A window that
  fails 3 times in a row is left for the gap's next re-read; the gap's next window is then
  read alone and left after one failed call.
- The last re-read has no next one. There a window that fails 3 times is kept and tried
  again, alone. It is given up only when a call more than a day after the last re-read
  time fails, so after that day it costs one call at most; each later window of the gap
  is then given up after one failed call. A re-read whose later times all went by while
  re-reads could not run is the last one too. A re-read that had begun before them and
  was cut short (the Pi off, a backend failing) is not: the last re-read still follows,
  and reads the whole gap again.
- Of the reads alone after the gaps read together, windows read for the last time get
  every other one while other windows wait too: windows kept on a gap's last re-read
  (see the previous point), and the other windows of a gap's last re-read that failed
  twice and are read alone. The tries of that day go by while they wait. A window whose
  last write a backend refused does not take that turn: the backend will most likely
  refuse it again (its points are older than a bucket's retention, say), and those tries
  would halve the speed of the other windows. It waits with them and still gets its
  calls; a later call that fails before the write (at the API, say) gives it the turn
  back.
- The other windows read alone go one gap at a time. In each of the two groups, windows
  not read alone yet go first, the most overdue first, then the gap that began first;
  then the one read alone least recently. A kept window whose last write a backend
  refused, more than a day after the last re-read time, counts as not read alone yet if
  its gap has a next window: its next failed call gives it up, and its gap goes on from
  there. A gap's next window after one that was left has not been read alone yet, so
  gaps whose windows all fail, such as gaps that reach beyond a bucket's retention (see
  "Limits"), are crawled through one after another: one window per pause of the reads
  alone (up to an hour), one per two pauses while a window that takes every other read
  is due. The windows that wait get their calls later. That costs nothing while the
  cloud and the bucket still hold their history, except for a window read for the last
  time whose last write a backend refused: it waits too, and the tries of its last day
  go by; after that day, one failed call gives it up. With a bucket retention, history
  can age out meanwhile (see "Limits").
- An answer cut off by `-q`/`--qlimit` is read again in smaller windows; one cut off even
  in 30-minute windows counts as a failed call.

The gaps are kept in `state.json` (key `rechecks`, ignored by older releases), so a
restart does not lose them; re-reads that fell due meanwhile are done once, together. A
sensor keeps at most 40 gaps, all sensors together 1,000; beyond that the closest gaps
are merged into one, so none is dropped.

`RECHECK_GAPS = 0` (or `off`, `no`, `none`, `false`, empty) turns the re-checks off: the
daemon then behaves exactly as without them, and `state.json` gets no new key. Legacy
config files (`~/.sensorpush.conf`, `~/.sensorpush_vm.conf`) may have a `[DAEMON]`
section for this; `RECHECK_GAPS` and `RECHECK_MIN_GAP` are the only keys read from it. An
invalid value stops the daemon at startup with a one-line error. One-shot runs ignore
both keys.

Limits:

- **Horizon.** Samples that reach the cloud more than 7 days after the gap closed are not
  fetched; a one-shot run with `-s`/`-p` still gets them.
- **On-board storage.** Sensors keep about 20 days (HT1), 30 days (HTP.xw) or 45 days
  (HT.w) of samples; older parts of a longer gap are not re-read.
- **Bucket retention.** InfluxDB 2 refuses a write that holds any point older than the
  bucket's retention (HTTP 422), so every re-read of a window that reaches beyond it
  fails. The re-reads get through such windows one failed call each. Each moves a gap on
  by one window, `-t` minus 60 minutes of data (11 hours at the default `-t 720`), while
  the retention boundary moves an hour per hour. History that ages out of the bucket
  meanwhile is lost, also that of the gaps that wait behind such a crawl. Measured on 18
  sensors (12 HT1, 2 HTP.xw, 4 HT.w), of the samples the bucket would still take when
  they came back: at `-t 720`, 91 % written after a 15-day outage with a 14-day bucket,
  42 % after a 10-day outage with a 7-day bucket; after the 15-day outage 48 % at `-t
  360`, 14 % at `-t 180`, and nothing at `-t 120` or less, where the re-reads never
  catch up. After a 21-day outage with a 14-day bucket, the 6 HT.w and HTP.xw gaps waited
  behind the 12 HT1 crawls, and only 150 of the 120,750 of their samples the bucket
  would take were written. A bucket that keeps longer than the longest outage plus 7
  days and an hour avoids all of this.
- **Short buckets.** A re-read writes only when it finds more samples than the reads
  before it, and it writes from an hour before the gap. With a bucket that keeps less than
  the last re-read time plus the gap and an hour (with the default `RECHECK_GAPS`, any
  bucket of 7 days or less), a last re-read that finds new samples - history uploaded
  after the 3d read, say - is refused: its write holds points older than the bucket keeps.
  Such a gap of 2 hours costs 29 failed calls over its last re-read and the day after it,
  at an `INTERVAL` of 5 minutes, each with one INFO, one WARNING and three ERROR lines,
  and is then given up. A gap whose history came by the 3d read costs nothing more: its
  last re-read finds nothing new and writes nothing. Set `RECHECK_GAPS` to times inside
  the retention, e.g. `1h, 6h, 1d, 3d` for a 7-day bucket.
- **A write refused for a moment.** A window read for the last time whose write a backend
  refused only for a moment (during a restart, say) loses its turn while other windows
  are read alone, such as those of a gap crawling through windows a bucket refuses: it
  waits with them, and the tries of its last day go by. It is lost if its single call
  after that day fails, or if, with a bucket retention, its history ages out while other
  gaps' windows go first. Measured: while the gap of a 30-day outage was crawled through
  with a 14-day bucket, a 2-hour gap whose last re-read a backend refused for 2 hours got
  its next call only 40 hours after its last re-read time.
- **Re-read calls failing for more than a day.** A gap's last re-read gives up a window
  that still fails more than a day after the last re-read time (see above). So when
  SensorPush answers the polls but fails the re-read calls for more than a day, a gap
  whose last re-read time came more than a day before that spell ends can lose the history
  only that re-read would write; which of these gaps lose depends on when their calls
  come. While the polls fail too, re-reads make no calls and give up nothing. Measured: 12
  HT1 gaps at their last re-read during a 27-hour spell of failing re-read calls, 86 to
  97 % of that history written.
- **HT1 data not seen live.** HT1 sensors seem to leave a reading out of their own record
  when neither temperature nor humidity has changed, so their own samples can be an hour
  apart. While the daemon polls at most 25 minutes apart it also sees the live copies, at
  most about 13 minutes apart, and a 20-minute silence is a real gap. In data it did not
  see live - from its own downtime, across a restart, or with polls more than 25 minutes
  apart - an HT1 gap counts only from 90 minutes, also in a stretch a poll did not reach.
  So an HT1 silence of 20 to 90 minutes while the daemon was down too (a power cut that
  took the Pi and the gateways down together) is not re-read. HTP.xw and HT.w sensors
  record every minute and always use `RECHECK_MIN_GAP`.
- **VictoriaMetrics duplicates.** Re-reads write some points again; with polls 45 to 59
  minutes apart, the reads of what a poll did not reach write about 80 % more points
  than the polls alone (measured on 18 sensors). InfluxDB overwrites them with identical
  values; VictoriaMetrics without `-dedup.minScrapeInterval` keeps them as duplicates, as
  it already does for the hour each poll overlaps the previous one. Averages, minimum and
  maximum are unaffected; `count()` is not.

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
- **Complete polls**: Every poll reaches back at least an hour (`POLL_BACKLOG`, default `1h`). The SensorPush API hands out each sensor's samples in chunks of about 15 minutes and selects a chunk by its *start* time, so a query from T returns only chunks that started at or after T. A shorter window silently misses most samples; shorter settings are raised to 1h with a warning. If you ran an earlier version with a 10-minute backlog, see [CHANGELOG.md](CHANGELOG.md) for how to back-fill what it missed. The newest minutes also come as live copies, which are stored too; see [Live and final samples](#live-and-final-samples)
- **Gap-filling**: If the API or database was unavailable, the daemon automatically detects the gap on the next successful cycle and back-fills the missing data. It finds the gap by asking each backend for its newest stored point, so the token needs read access as well as write access. If a backend cannot be asked (a timeout, missing permission, or it is failing writes at that moment), the daemon resumes from the end of the last collection known to have completed (kept in `state.json`) instead of assuming an empty database. That mark is kept for the collector as a whole: a backend that rejected writes while another one kept working catches up once it accepts writes and answers lookups again. VictoriaMetrics needs v1.52 or later (`tlast_over_time()`)
- **Gap re-checks**: Gateways upload the samples an outage missed late, so the daemon reads every gap in a sensor's data again 1h, 6h, 1d, 3d and 7d after it closed (`RECHECK_GAPS`, `0` turns it off). Without gaps this costs no API calls at an `INTERVAL` of 30 minutes or less, or of 60 minutes or more with the default `DEEP_SCAN_INTERVAL`; see [Gap re-checks](#gap-re-checks)
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

The individual scripts are still available but deprecated in favor of `sensorpushd.py`. They are kept as they are; changing what they store would split their users' series. Do not run them alongside the daemon against the same database: they store `sensor_id` in a different form.

| Script | Database | Config File | Stores `sensor_id` as |
|--------|----------|-------------|-----------------------|
| `sensorpush.py` | InfluxDB 1.x | `~/.sensorpush_legacy.conf` | full id |
| `sensorpush2.py` | InfluxDB 2.x | `~/.sensorpush.conf` | `str(float(id))`, like `sensorpushd.py` |
| `sensorpush_vm.py` | VictoriaMetrics | `~/.sensorpush_vm.conf` | full id |

`sensorpushd.py` has no InfluxDB 1.x backend, so `sensorpush.py` has no direct replacement.

## Grafana Dashboards

Example Grafana dashboards are included:
- `SensorPush-GrafanaDashboard.json` - Dashboard for InfluxDB
- `SensorPush-GrafanaDashboard-vm.json` - Dashboard for VictoriaMetrics (Prometheus data source)

Both select sensors by `sensor_name` and combine all series of a name into one line, so a
renamed sensor, a switch of the `sensor_id` form or enabling `SENSOR_UID` does not show up
as duplicate lines. (A dashboard you imported earlier is not updated by pulling a new
version of the file; re-import it to get the change.)

## Migration from InfluxDB to VictoriaMetrics

If you have existing SensorPush data in InfluxDB and want to migrate to VictoriaMetrics, use the `migrate_influx2vm.py` script:

```
# migrate_influx2vm.py --help
usage: migrate_influx2vm.py [-h] [-c CONFIG] [--ifdb-config IFDB_CONFIG] [--vm-config VM_CONFIG]
                            [--start START_TIME] [--end END_TIME] [--all]
                            [--batch-size BATCH_SIZE] [--chunk-days CHUNK_DAYS]
                            [--dry-run] [--verbose]

Migrate SensorPush data from InfluxDB 2.x to VictoriaMetrics

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Read both databases from a unified sensorpushd.py config
                        ([INFLUXDB2] and [VICTORIAMETRICS] sections)
  --ifdb-config IFDB_CONFIG
                        Path to InfluxDB config file (default: ~/.sensorpush.conf)
  --vm-config VM_CONFIG
                        Path to VictoriaMetrics config file (default: ~/.sensorpush_vm.conf)
  --start START_TIME    Start time (ISO format, or relative written as --start=-7d)
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

# Migrate everything since a date
python migrate_influx2vm.py --start="2026-01-01T00:00:00Z"

# Migrate the last 30 days (relative times need the '=' form)
python migrate_influx2vm.py --start=-30d

# Preview migration without writing, reading the daemon's unified config
python migrate_influx2vm.py --config /etc/sensorpushd.conf --all --dry-run
```

The script copies `sensor_id` exactly as InfluxDB holds it, so history from different
scripts keeps its form (see [Stored sensor ids](#stored-sensor-ids)).

## CSV import
```
# sensorpush_csv-import.py --help
usage: sensorpush_csv-import.py [-h] [-f CSVFILE] [-s SENSORNAME]
                                [-i SENSORID] [--float-id] [-d] [-c CHUNKS]

Reads a CSV file exported from the SensorPush Android App and stores the temp
and humidity readings in InfluxDB

optional arguments:
  -h, --help            show this help message and exit
  -f CSVFILE, --csvfile CSVFILE
                        CSV file exported from the SensorPush Android App
  -s SENSORNAME, --sensorname SENSORNAME
                        Sensor name
  -i SENSORID, --sensorid SENSORID
                        Sensor id, in the form your collector stores it, so the
                        import lands in the same series
  --float-id            Store the id the way sensorpushd.py and sensorpush2.py do,
                        str(float(id)), e.g. 123456.7890123456789012 -> 123456.78901234567
  -d, --dryrun          Do not write anything to InfluxDB - just print what
                        would have been written
  -c CHUNKS, --chunks CHUNKS
                        Write data in chunks to InfluxDB to not overload e.g.
                        a RaspberryPi
```
