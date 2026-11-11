"""Gap re-checks: detection, holes, scheduling, re-reads, state and config.

Everything runs against a fake cloud and a fake clock: inside the daemon
module, time.time(), time.monotonic() and datetime.now() follow Clock, and
Cloud answers get_samples() from the sample times it holds.

Comments that name a review item (load-r4-4, compat-r2-1, ...) or a round
(round 3, ...) refer to the reviews of this feature before its first
release: they say which earlier behaviour a test guards against.
"""
import configparser
import copy
import datetime
import json
import logging
import re
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from helpers import (DaemonTestCase, ID_A, ID_B, ID_C, UTC, fake_writer,
                     make_args, sensorpushd)

spd = sensorpushd()
TAG_A, TAG_B = spd.sensor_id_tag(ID_A), spd.sensor_id_tag(ID_B)
T0 = datetime.datetime(2026, 11, 8, 10, 0, tzinfo=UTC).timestamp()
M, H, D = 60, 3600, 86400
OFFSETS = [H, 6 * H, D, 3 * D, 7 * D]


def at(t):
    return datetime.datetime.fromtimestamp(t, UTC)


def iso(t):
    """An 'observed' time as the API writes it: UTC, milliseconds, 'Z'."""
    dt = at(t)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.') + '%03dZ' % (dt.microsecond // 1000)


def every(start, stop, step=M):
    """Sample times from start to stop, both included, step seconds apart."""
    return [start + k * step for k in range(int(round((stop - start) / step)) + 1)]


class Clock:
    """The time the daemon sees: wall clock and monotonic, moved by hand."""

    def __init__(self, t):
        self.t = float(t)
        self.mono = 5000.0

    def advance(self, seconds):
        self.t += seconds
        self.mono += seconds


class TimeShim:
    """The daemon module's ``time``, with time() and monotonic() from a Clock."""

    def __init__(self, clock):
        self._clock = clock

    def time(self):
        return self._clock.t

    def monotonic(self):
        return self._clock.mono

    def __getattr__(self, name):
        return getattr(time, name)


def datetime_shim(clock):
    """The daemon module's ``datetime``, with datetime.now() from a Clock."""

    class ClockDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime.fromtimestamp(clock.t, tz)

    return types.SimpleNamespace(datetime=ClockDateTime, date=datetime.date,
                                 timedelta=datetime.timedelta,
                                 timezone=datetime.timezone)


class Cloud:
    """The SensorPush cloud, as far as the daemon asks it.

    Holds sample times per sensor and answers get_samples() with each
    sensor's samples observed within [start, stop] and not in the future,
    newest first (the daemon must not rely on the order).  ``fail`` makes
    that many calls raise, ``truncate`` marks the answers truncated, and
    each call takes ``call_seconds`` of the clock.  Calls are recorded with
    ``tag`` (set while re-reads run, see RecheckCase.tag_rechecks).
    """

    def __init__(self, clock, sensor_types):
        self.clock = clock
        self.types = dict(sensor_types)
        self.samples = {sid: set() for sid in self.types}
        self.calls = []
        self.fail = 0
        self.truncate = False
        self.call_seconds = 0
        self.tag = None

    def add(self, sid, times):
        self.samples[sid].update(times)

    def sensors(self):
        return {sid: {'id': sid, 'name': f'S{i}', 'type': t,
                      'battery_voltage': 3.0, 'rssi': -60}
                for i, (sid, t) in enumerate(self.types.items())}

    def answer(self, start, stop, sensors=None, limit=10000):
        lo = spd._observed_epoch(start)
        hi = min(spd._observed_epoch(stop), self.clock.t)
        data = {}
        for sid, times in self.samples.items():
            if sensors and sid not in sensors:
                continue
            chosen = sorted(t for t in times if lo <= t <= hi)[:limit]
            if chosen:
                data[sid] = [{'observed': iso(t), 'temperature': 68.0,
                              'humidity': 40.0} for t in reversed(chosen)]
        return {'sensors': data, 'truncated': self.truncate,
                'total_samples': sum(len(v) for v in data.values()),
                'total_sensors': len(data)}

    # -- the SensorPushAPI interface the daemon uses --
    def _ensure_auth(self):
        pass

    def get_sensors(self):
        self.calls.append(('get_sensors', self.tag))
        return self.sensors()

    def get_reports(self):
        self.calls.append(('get_reports', self.tag))
        return {}

    def get_samples(self, start, stop, measures=None, limit=0, sensors=None):
        self.calls.append(('get_samples', self.tag, start, stop,
                           tuple(sensors or ()), limit))
        self.clock.advance(self.call_seconds)
        if self.fail:
            self.fail -= 1
            raise ValueError('API request failed with status 500')
        return self.answer(start, stop, sensors, limit or 10)

    def sample_calls(self, tag=None):
        return [c for c in self.calls if c[0] == 'get_samples' and c[1] == tag]


class LimitCloud(Cloud):
    """A Cloud that marks an answer truncated when the limit cut a sensor."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.truncated_calls = 0

    def get_samples(self, start, stop, measures=None, limit=0, sensors=None):
        answer = super().get_samples(start, stop, measures, limit, sensors)
        full = self.answer(start, stop, sensors, 10 ** 9)
        answer['truncated'] = any(len(v) > (limit or 10)
                                  for v in full['sensors'].values())
        self.truncated_calls += answer['truncated']
        return answer


class ChunkCloud(Cloud):
    """A Cloud that selects the newest hour by 15-min chunk start, as the
    API does: a window starting at T returns those samples only from the
    first chunk that starts at or after T."""

    def answer(self, start, stop, sensors=None, limit=10000):
        out = super().answer(start, stop, sensors, limit)
        lo, now = spd._observed_epoch(start), self.clock.t
        for sid in list(out['sensors']):
            kept = [i for i in out['sensors'][sid]
                    if now - spd._observed_epoch(i['observed']) >= H
                    or spd._observed_epoch(i['observed']) // (15 * M) * (15 * M) >= lo]
            if kept:
                out['sensors'][sid] = kept
            else:
                del out['sensors'][sid]
        out['total_samples'] = sum(len(v) for v in out['sensors'].values())
        out['total_sensors'] = len(out['sensors'])
        return out


class RecheckCase(DaemonTestCase):
    """A daemon on a fake clock, with its log captured."""

    def setUp(self):
        super().setUp()
        self.clock = Clock(T0)
        self.sleeps = []
        for name, shim in (('time', TimeShim(self.clock)),
                           ('datetime', datetime_shim(self.clock))):
            patcher = mock.patch.object(spd, name, shim)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.records = []
        handler = logging.Handler()
        handler.emit = self.records.append
        log = logging.getLogger('sensorpushd')
        saved = (log.level, log.propagate)
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
        log.propagate = False

        def restore():
            log.removeHandler(handler)
            log.setLevel(saved[0])
            log.propagate = saved[1]
        self.addCleanup(restore)

    def logged(self, level, text=''):
        return [r.getMessage() for r in self.records
                if r.levelno == level and text in r.getMessage()]

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.clock.advance(seconds)

    def daemon(self, cloud, writer=None, args=None, **daemon_cfg):
        d = self.make_daemon([writer or fake_writer()], api=cloud,
                             args=args or make_args(), **daemon_cfg)
        d._interruptible_sleep = self.sleep
        return d

    def fetch(self, d, cloud, start, stop=None):
        """Let the daemon see the window [start, stop] (default: now)."""
        stop = self.clock.t if stop is None else stop
        window = [d._window_str(start), d._window_str(stop)]
        d._note_fetched(cloud.answer(*window), cloud.sensors(), window)

    def tag_rechecks(self, d, cloud):
        """Record the calls made by re-reads with the tag 'recheck'."""
        original = d._recheck_gaps

        def tagged(*a, **k):
            cloud.tag = 'recheck'
            try:
                return original(*a, **k)
            finally:
                cloud.tag = None
        d._recheck_gaps = tagged

    def reset_state(self):
        """Start over: clock, state file and freshness marker."""
        self.clock.t, self.clock.mono = T0, 5000.0
        for name in ('state.json', 'last-success'):
            path = self.state_dir / name
            if path.exists():
                path.unlink()

    def write_state(self, holes, **extra):
        state = {'sensors': {}, 'failed_windows': [], 'collected_until': None,
                 'rechecks': {'last_seen': {}, 'holes': holes}}
        state.update(extra)
        (self.state_dir / 'state.json').write_text(json.dumps(state))
        return state


def written_times(writer, measurement='SensorPush'):
    """Observed times of the sample records a fake writer got; clears it."""
    times = sorted(spd._observed_epoch(r['time']) for batch in writer.written
                   for r in batch if r['measurement'] == measurement)
    del writer.written[:]
    return times


# -----------------------------------------------------------------------------
# Time parsing
# -----------------------------------------------------------------------------
class ObservedTimes(unittest.TestCase):

    FORMS = ('2026-11-08T10:00:00.000Z', '2026-11-08T10:00:00Z',
             '2026-11-08T10:00:00+00:00', '2026-11-08T10:00:00.000+00:00',
             '2026-11-08T11:00:00+0100', '2026-11-08T05:00:00-0500',
             '2026-11-08T10:00:00', '2026-11-08T10:00:00.000')

    def check(self):
        for text in self.FORMS:
            self.assertEqual(spd._observed_epoch(text), T0, text)
        self.assertEqual(spd._observed_epoch('2026-11-08T10:00:00.5Z'), T0 + 0.5)
        self.assertEqual(spd._observed_epoch(' 2026-11-08T10:00:00.000Z '), T0)

    def test_api_window_and_naive_forms(self):
        self.check()

    def test_unparseable_values_give_none(self):
        for value in (None, '', 'soon', '2026-13-08T10:00:00Z', T0,
                      b'2026-11-08T10:00:00Z'):
            self.assertIsNone(spd._observed_epoch(value), value)

    def test_log_formats(self):
        # Local time, whatever the zone of the machine running the tests.
        start = datetime.datetime(2026, 11, 8, 3, 0).timestamp()
        end = datetime.datetime(2026, 11, 8, 5, 10).timestamp()
        self.assertEqual(spd._fmt_span(start, end, end + H), '03:00-05:10')
        self.assertEqual(spd._fmt_span(start, end, end + D), '2026-11-08 03:00-05:10')
        self.assertEqual(spd._fmt_span(start, end + D, end + D),
                         '2026-11-08 03:00 - 2026-11-09 05:10')
        self.assertEqual(spd._fmt_duration(end - start), '130 min')
        self.assertEqual(spd._fmt_duration(7.5 * H), '7.5 h')
        self.assertEqual(spd._fmt_duration(3 * D), '3.0 days')
        self.assertEqual([spd._fmt_offset(o) for o in [30 * M, 90 * M] + OFFSETS],
                         ['30m', '90m', '1h', '6h', '1d', '3d', '7d'])

    def test_window_strings_round_trip(self):
        for t in (T0, T0 + 0.75 * D, T0 - 120 * D):     # also across DST
            self.assertEqual(spd._observed_epoch(
                spd.SensorPushDaemon._window_str(t)), t)

    def test_fromisoformat_of_python_before_3_11(self):
        # Before 3.11, fromisoformat() rejects a trailing 'Z', '+HHMM' and
        # fractions that are not 3 or 6 digits long.
        rejected = []

        class Strict(datetime.datetime):
            @classmethod
            def fromisoformat(cls, text):
                if (text.endswith('Z') or re.search(r'[+-]\d{4}$', text)
                        or re.search(r'\.(\d{1,2}|\d{4,5})(?=[+-]|$)', text)):
                    rejected.append(text)
                    raise ValueError(f'Invalid isoformat string: {text!r}')
                return super().fromisoformat(text)

        shim = types.SimpleNamespace(datetime=Strict, date=datetime.date,
                                     timedelta=datetime.timedelta,
                                     timezone=datetime.timezone)
        with mock.patch.object(spd, 'datetime', shim):
            self.check()
        # 'Z' and '+0100' never reach it; only the one-digit fraction needs
        # the strptime() fallback.
        self.assertEqual(rejected, ['2026-11-08T10:00:00.5+00:00'])


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
LEGACY_IFDB2 = """\
[SONSORPUSHAPI]
LOGIN = a
PASSWD = b
[INFLUXDBCONF]
IFDB_URL = http://localhost
IFDB_PORT = 8086
IFDB_TOKEN = t
IFDB_ORG = o
IFDB_BUCKET = b
[MISC]
MY_ALTITUDE = 0
"""
LEGACY_VM = """\
[SENSORPUSHAPI]
LOGIN = a
PASSWD = b
[VICTORIAMETRICSCONF]
VM_URL = http://localhost:8428
[MISC]
MY_ALTITUDE = 0
"""
UNIFIED = """\
[SENSORPUSHAPI]
LOGIN = a
PASSWORD = b
[BACKEND]
TYPE = influxdb2
[INFLUXDB2]
TOKEN = t
"""


def load_config(text):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'test.conf'
        path.write_text(text)
        return spd.load_config(str(path))


class RecheckConfig(RecheckCase):

    def test_defaults(self):
        self.assertEqual(spd.parse_recheck_gaps(
            spd.DEFAULT_RECHECK_CFG['recheck_gaps']), OFFSETS)
        d = self.daemon(Cloud(self.clock, {}))
        self.assertTrue(d._recheck_on)
        self.assertEqual(d._recheck_offsets, OFFSETS)
        self.assertEqual(d._recheck_min_gap, 20 * M)
        self.assertTrue(self.logged(logging.INFO, 'gaps over 20m are read again '
                                                  '1h, 6h, 1d, 3d, 7d after'))
        self.assertFalse(self.daemon(Cloud(self.clock, {}),
                                     args=make_args(daemon=False))._recheck_on)
        # Not [DAEMON] defaults: a config without the keys loads as before.
        self.assertFalse(set(spd.DEFAULT_RECHECK_CFG) & set(spd.DEFAULT_DAEMON_CFG))

    def test_disable_spellings(self):
        for value in ('', '0', 'off', 'OFF', 'False', 'no', 'None', '  none  '):
            self.assertEqual(spd.parse_recheck_gaps(value), [], repr(value))
            # RECHECK_MIN_GAP does not matter then, as it did not before.
            d = self.daemon(Cloud(self.clock, {}), recheck_gaps=value,
                            recheck_min_gap='junk')
            self.assertFalse(d._recheck_on, repr(value))
        self.assertEqual(self.logged(logging.ERROR), [])

    def test_invalid_values_exit_at_startup(self):
        cloud = Cloud(self.clock, {ID_A: 'HT1'})
        invalid = [('recheck_gaps', v) for v in (
            'soon', '1x', '6H', '0h', '-1h', '91d', '3M', '1h,,6h', '1h 6h', '1h,')]
        invalid += [('recheck_min_gap', v) for v in ('abc', '0m', '-5m', '', '20')]
        for key, value in invalid:
            del self.records[:]
            with self.assertRaises(SystemExit) as cm:
                self.daemon(cloud, **{key: value})
            self.assertEqual(cm.exception.code, 2, (key, value))
            errors = self.logged(logging.ERROR)
            self.assertEqual(len(errors), 1, (key, value))
            self.assertIn(key.upper(), errors[0])
            self.assertNotIn('\n', errors[0])
        self.assertEqual(cloud.calls, [])
        self.assertFalse((self.state_dir / 'state.json').exists())
        # One-shot mode ignores both keys.
        self.daemon(cloud, args=make_args(daemon=False), recheck_gaps='soon',
                    recheck_min_gap='abc')

    def test_floors_with_one_warning(self):
        for parse, value, expected, floor in (
                (spd.parse_recheck_gaps, '10m, 1h', [30 * M, H], '30m'),
                (spd.parse_recheck_gaps, '5m, 10m, 20m', [30 * M], '30m'),
                (spd.parse_recheck_min_gap, '5m', 15 * M, '15m')):
            del self.records[:]
            self.assertEqual(parse(value), expected)
            warnings = self.logged(logging.WARNING)
            self.assertEqual(len(warnings), 1, value)
            self.assertIn('raised to ' + floor, warnings[0])
        del self.records[:]
        self.assertEqual(spd.parse_recheck_gaps('30m'), [30 * M])
        self.assertEqual(spd.parse_recheck_min_gap('15m'), 15 * M)
        self.assertEqual(self.logged(logging.WARNING), [])

    def test_whitespace_order_and_duplicates(self):
        self.assertEqual(spd.parse_recheck_gaps(' 1h ,6h,\t1d '), [H, 6 * H, D])
        self.assertEqual(spd.parse_recheck_gaps('7d, 1h, 1h, 60m'), [H, 7 * D])
        self.assertEqual(spd.parse_recheck_min_gap(' 30m '), 30 * M)

    def test_legacy_files_with_and_without_daemon_section(self):
        for base in (LEGACY_IFDB2, LEGACY_VM):
            plain = load_config(base)
            self.assertEqual(plain['daemon'], dict(spd.DEFAULT_DAEMON_CFG))
            d = self.daemon(Cloud(self.clock, {}), **plain['daemon'])
            self.assertEqual(d._recheck_offsets, OFFSETS)
            cfg = load_config(base + '[DAEMON]\nRECHECK_GAPS = 0\n'
                              'RECHECK_MIN_GAP = 30m\nINTERVAL = 60\n')
            self.assertEqual(cfg['daemon']['recheck_gaps'], '0')
            self.assertEqual(cfg['daemon']['recheck_min_gap'], '30m')
            # The only [DAEMON] keys a legacy file honours.
            self.assertEqual(cfg['daemon']['interval'], 300)
            self.assertFalse(self.daemon(Cloud(self.clock, {}),
                                         **cfg['daemon'])._recheck_on)

    def test_a_percent_sign_reaches_the_parsers(self):
        # configparser interpolates '%': read that way, '50%' failed loading
        # the whole file, in one-shot mode and --listsensors as well (compat-2).
        for base in (UNIFIED, LEGACY_IFDB2, LEGACY_VM):
            for key, value in (('RECHECK_GAPS', '1h, 50%'), ('RECHECK_MIN_GAP', '20%')):
                with self.subTest(key=key, unified=base is UNIFIED):
                    cfg = load_config(base + '[DAEMON]\n%s = %s\n' % (key, value))
                    self.assertEqual(cfg['daemon'][key.lower()], value)
                    del self.records[:]
                    with self.assertRaises(SystemExit) as cm:
                        self.daemon(Cloud(self.clock, {}), **cfg['daemon'])
                    self.assertEqual(cm.exception.code, 2)
                    errors = self.logged(logging.ERROR)
                    self.assertEqual(len(errors), 1)
                    self.assertIn(key, errors[0])
                    self.assertNotIn('\n', errors[0])
                    # One-shot mode ignores both keys.
                    self.assertFalse(self.daemon(Cloud(self.clock, {}),
                                                 args=make_args(daemon=False),
                                                 **cfg['daemon'])._recheck_on)

    def test_unified_file(self):
        without = load_config(UNIFIED + '[DAEMON]\nINTERVAL = 300\n')
        self.assertFalse(set(spd.DEFAULT_RECHECK_CFG) & set(without['daemon']))
        cfg = load_config(UNIFIED + '[DAEMON]\nRECHECK_GAPS = 1d, 1h\n'
                          'RECHECK_MIN_GAP = 25m\n')
        self.assertEqual(cfg['daemon']['recheck_gaps'], '1d, 1h')
        d = self.daemon(Cloud(self.clock, {}), **cfg['daemon'])
        self.assertEqual((d._recheck_offsets, d._recheck_min_gap), ([H, D], 25 * M))

    def test_template_documents_both_keys_commented_out(self):
        self.assertIn('# RECHECK_GAPS = 1h, 6h, 1d, 3d, 7d',
                      spd.DEFAULT_CONFIG_TEMPLATE)
        self.assertIn('# RECHECK_MIN_GAP = 20m', spd.DEFAULT_CONFIG_TEMPLATE)
        parser = configparser.ConfigParser()
        parser.read_string(spd.DEFAULT_CONFIG_TEMPLATE)
        self.assertFalse(parser.has_option('DAEMON', 'RECHECK_GAPS'))


# -----------------------------------------------------------------------------
# Detection
# -----------------------------------------------------------------------------
class Detection(RecheckCase):

    def setUp(self):
        super().setUp()
        self.cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1'})
        self.d = self.daemon(self.cloud)

    def holes(self, d=None):
        return [(h['sid'], h['start'], h['end'])
                for h in (d or self.d)._recheck_holes]

    def test_gap_inside_one_response(self):
        now = self.clock.t
        self.cloud.add(ID_A, every(now - 3 * H, now - 2 * H) + every(now - H, now))
        self.fetch(self.d, self.cloud, now - 3 * H)
        self.assertEqual(self.holes(), [(ID_A, now - 2 * H, now - H)])
        hole = self.d._recheck_holes[0]
        self.assertEqual((hole['closed_at'], hole['done'], hole['pos']), (now, 0, 0))
        # -t 720: 11-h cells, so a call window is 12 h like every other one;
        # an hour's gap plus the margins is one cell.
        self.assertEqual((hole['cell'], hole['counts']), (660, [0]))
        self.assertEqual(self.d._recheck_last_seen, {ID_A: now})
        self.assertEqual(self.logged(logging.INFO, 'Gap in'), [
            "Gap in 'S0': no data %s (60 min); re-reading it after 1h, 6h, 1d, "
            "3d, 7d" % spd._fmt_span(now - 2 * H, now - H, now)])

    def test_gap_between_last_seen_and_a_response(self):
        now = self.clock.t
        self.d._recheck_last_seen[ID_A] = now - 2 * H
        self.cloud.add(ID_A, every(now - 50 * M, now))
        self.fetch(self.d, self.cloud, now - H)
        self.assertEqual(self.holes(), [(ID_A, now - 2 * H, now - 50 * M)])

    def test_continuous_data_and_windows_fetched_again_give_no_hole(self):
        now = self.clock.t
        self.cloud.add(ID_A, every(now - 5 * H, now))
        self.cloud.add(ID_B, every(now - 5 * H, now, 7 * M))
        self.fetch(self.d, self.cloud, now - 5 * H)
        self.fetch(self.d, self.cloud, now - 3 * H)
        self.fetch(self.d, self.cloud, now - 5 * H, now - 4 * H)
        self.assertEqual(self.holes(), [])
        self.assertEqual(self.d._recheck_last_seen[ID_A], now)

    def test_threshold_boundary(self):
        now = self.clock.t
        # Records every minute: RECHECK_MIN_GAP (20 min), exactly.
        self.cloud.add(ID_A, every(now - 6 * H, now - 5 * H)
                       + every(now - 5 * H + 20 * M, now - 4 * H)
                       + every(now - 4 * H + 20 * M + 1, now - 3 * H + 1))
        # HT1 in data that was not seen live: RECHECK_SETTLED_MIN_GAP (90 min).
        self.cloud.add(ID_B, every(now - 10 * H, now - 9 * H, 30 * M)
                       + every(now - 9 * H + 90 * M, now - 6 * H, 30 * M)
                       + every(now - 6 * H + 90 * M + 1, now - 3 * H + 1, 30 * M))
        self.fetch(self.d, self.cloud, now - 11 * H, now - 2 * H)
        self.assertEqual(self.holes(), [(ID_A, now - 4 * H, now - 4 * H + 20 * M + 1),
                                        (ID_B, now - 6 * H, now - 6 * H + 90 * M + 1)])

    def test_newest_stored_point_is_the_baseline_on_the_first_cycle(self):
        # First cycle after an upgrade: no last_seen yet, but the deep scan
        # has found the newest stored point - here a live copy, newer than
        # the own samples the cloud now serves before the gap.
        now = self.clock.t
        cloud = Cloud(self.clock, {ID_A: 'HT.w'})
        cloud.add(ID_A, every(now - 4 * H, now - 3 * H - 2 * M)
                  + every(now - 40 * M, now))
        d = self.daemon(cloud, writer=fake_writer(newest={TAG_A: at(now - 3 * H)}))
        d._collect_cycle()
        self.assertEqual(self.holes(d), [(ID_A, now - 3 * H, now - 40 * M)])
        # Without any baseline - a new sensor - the first sample is one.
        cloud.types[ID_B] = 'HT.w'
        cloud.samples[ID_B] = set(every(self.clock.t - 30 * M, self.clock.t))
        self.fetch(d, cloud, self.clock.t - H)
        self.assertEqual(len(d._recheck_holes), 1)
        self.assertEqual(d._recheck_last_seen[ID_B], self.clock.t)

    def test_unreadable_observed_times_are_skipped_and_reported_once(self):
        now = self.clock.t
        self.cloud.add(ID_A, every(now - 3 * H, now - 2 * H) + every(now - H, now))
        window = [self.d._window_str(now - 3 * H), self.d._window_str(now)]
        answer = self.cloud.answer(*window)
        answer['sensors'][ID_A][:0] = [{'observed': 'soon'}, {'temperature': 1.0}]
        for _ in range(2):
            self.d._note_fetched(answer, self.cloud.sensors(), window)
        self.assertEqual(self.holes(), [(ID_A, now - 2 * H, now - H)])
        warnings = self.logged(logging.WARNING)
        self.assertEqual(len(warnings), 1)
        self.assertIn("Sensor 'S0': 2 sample(s) without a readable observed time",
                      warnings[0])

    def test_samples_from_the_future_do_not_hold_last_seen(self):
        # One sample with a wrong clock must not hide every gap until then.
        now = self.clock.t
        self.cloud.add(ID_A, every(now - H, now))
        window = [self.d._window_str(now - H), self.d._window_str(now)]
        answer = self.cloud.answer(*window)
        answer['sensors'][ID_A].append({'observed': iso(now + 365 * D)})
        self.d._note_fetched(answer, self.cloud.sensors(), window)
        self.assertEqual(self.d._recheck_last_seen[ID_A], now)
        self.assertEqual(len(self.logged(logging.WARNING, 'observed in the future')), 1)

    def test_sensorlist(self):
        d = self.daemon(self.cloud, args=make_args(sensorlist=[ID_B]))
        now = self.clock.t
        for sid in (ID_A, ID_B):
            self.cloud.add(sid, every(now - 6 * H, now - 4 * H) + every(now - H, now))
        self.fetch(d, self.cloud, now - 6 * H)
        self.assertEqual([h['sid'] for h in d._recheck_holes], [ID_B])
        self.assertEqual(set(d._recheck_last_seen), {ID_B})

    def test_nothing_in_one_shot_mode_or_when_off(self):
        now = self.clock.t
        self.cloud.add(ID_A, every(now - 3 * H, now - 2 * H) + every(now - H, now))
        for kwargs in ({'args': make_args(daemon=False)}, {'recheck_gaps': '0'}):
            d = self.daemon(self.cloud, **kwargs)
            d._note_fetched = mock.Mock()
            window = [d._window_str(now - 3 * H), d._window_str(now)]
            self.assertTrue(d._fetch_and_write_window(
                window, 1, 1, self.cloud.sensors(), 'SensorPush'))
            d._note_fetched.assert_not_called()
            self.assertEqual((d._recheck_holes, d._recheck_last_seen), ([], {}))


class LiveAndSettled(RecheckCase):

    def test_ht1_skip_in_a_catch_up_after_a_pause_is_no_gap(self):
        cloud = Cloud(self.clock, {ID_B: 'HT1'})
        now = self.clock.t
        # Own records skip unchanged readings: an hour without one.
        cloud.add(ID_B, every(now - 4 * H, now - 3 * H, 5 * M)
                  + every(now - 2 * H, now, 5 * M))
        d = self.daemon(cloud)
        d._recheck_last_seen[ID_B] = now - 4 * H    # the daemon was away since
        self.fetch(d, cloud, now - 5 * H)           # the catch-up reaches now
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(d._recheck_last_seen[ID_B], now)

    def test_the_same_skip_observed_live_is_a_gap(self):
        cloud = Cloud(self.clock, {ID_B: 'HT1'})
        t0 = self.clock.t
        # Seen as live copies every 5 minutes, then nothing for an hour.
        cloud.add(ID_B, every(t0 - H, t0 + H, 5 * M)
                  + every(t0 + 2 * H, t0 + 3 * H, 5 * M))
        d = self.daemon(cloud)
        while self.clock.t <= t0 + 3 * H:           # a poll every 5 minutes
            self.fetch(d, cloud, self.clock.t - H)
            self.clock.advance(5 * M)
        self.assertEqual([(h['start'], h['end']) for h in d._recheck_holes],
                         [(t0 + H, t0 + 2 * H)])

    def test_htp_xw_jump_in_settled_data_is_a_gap(self):
        cloud = Cloud(self.clock, {ID_C: 'HTP.xw'})
        now = self.clock.t
        cloud.add(ID_C, every(now - 5 * H, now - 4 * H)
                  + every(now - 4 * H + 25 * M, now - 3 * H))
        d = self.daemon(cloud)
        self.fetch(d, cloud, now - 6 * H, now - 2 * H)  # not seen live
        self.assertEqual([(h['start'], h['end']) for h in d._recheck_holes],
                         [(now - 4 * H, now - 4 * H + 25 * M)])


class WindowStart(RecheckCase):
    """A fetched window that starts after a sensor's newest sample seen so
    far leaves the stretch between unread: after a silence of about 45-75
    min whose backlog arrived before any poll saw the return, after the
    daemon paused, and - the API selects the newest hour by 15-min chunk
    start - in the first chunk of every window there."""

    def start(self, cloud, **kw):
        self.w = fake_writer()
        self.db = set()
        return self.restart(cloud, **kw)

    def restart(self, cloud, **kw):
        """A daemon process on the state directory, with the same database."""
        d = self.daemon(cloud, writer=self.w, **kw)
        self.tag_rechecks(d, cloud)
        return d

    def cycle(self, d, step=5 * M):
        """One cycle, then the database's newest points follow the writes."""
        d._collect_cycle()
        for batch in self.w.written:
            for r in batch:
                if r['measurement'] != 'SensorPush':
                    continue
                t = spd._observed_epoch(r['time'])
                tag = r['tags']['sensor_id']
                self.db.add((tag, t))
                if self.w.newest.get(tag) is None or at(t) > self.w.newest[tag]:
                    self.w.newest[tag] = at(t)
        del self.w.written[:]
        self.clock.advance(step)

    def missing(self, cloud, sid, lo, hi):
        """Samples the cloud holds strictly inside (lo, hi) but not stored."""
        tag = spd.sensor_id_tag(sid)
        return sorted(t for t in cloud.samples[sid]
                      if lo < t < hi and (tag, t) not in self.db)

    def test_silence_whose_backlog_came_before_the_window_reached_it(self):
        # An HT.w silent for 65 min; its backlog reaches the cloud a minute
        # after it returns, before the next poll.  That poll starts an hour
        # back, 5 min after the last sample seen: the jump to its first
        # sample is short, but the 4 samples before it were never fetched.
        cloud = Cloud(self.clock, {ID_A: 'HT.w'})
        d = self.start(cloud)
        p0 = T0 + 2 * H
        cloud.add(ID_A, every(T0 - 3 * H, p0))
        while self.clock.t < p0 + 65 * M:
            self.cycle(d)
        cloud.add(ID_A, every(p0 + M, self.clock.t))     # back, backlog in
        self.cycle(d)
        self.assertEqual([(h['start'], h['end'], h['probe']) for h in d._recheck_holes],
                         [(p0, p0 + 5 * M, 20 * M)])
        self.assertEqual(len(self.missing(cloud, ID_A, p0, p0 + 65 * M)), 4)
        self.assertEqual(self.logged(logging.INFO, 'Gap in'), [])
        # At DEBUG: with an INTERVAL of 40-59 min this is most cycles.
        self.assertEqual(self.logged(logging.INFO, 'right after the newest sample'), [])
        self.assertEqual(len(self.logged(logging.DEBUG, 'right after the newest sample')), 1)
        end = self.clock.t + 6 * H
        while self.clock.t < end:
            cloud.add(ID_A, [self.clock.t])
            self.cycle(d)
        self.assertEqual(self.missing(cloud, ID_A, p0, p0 + 65 * M), [])
        # Complete at the first read: one call, then dropped.
        self.assertEqual(len(cloud.sample_calls('recheck')), 1)
        self.assertEqual(d._recheck_holes, [])
        # What the probe found is worth an INFO line - one for the call, in
        # words that do not speak of a gap (compat-r2-1).
        self.assertEqual(self.logged(logging.INFO, 'Re-read'), [])
        found = self.logged(logging.INFO, 'Read again')
        self.assertEqual(len(found), 1)
        self.assertRegex(found[0], r'^Read again what the polls may have missed for '
                                   r'1 sensor\(s\) \(.+ -> .+\): 4 new samples, \d+ written$')

    def test_ht1_silent_before_a_restart_whose_backlog_came_meanwhile(self):
        # An HT1 seen live for the last time 100 min before the daemon
        # stopped; its backlog arrived while the daemon was down for 2 h.
        # The catch-up starts an hour before the stop: the 35 min of HT1
        # data before that are not in it, and as data not seen live the
        # jump is far below the 90-min threshold.
        cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1'})
        d = self.start(cloud)
        stop = T0 + 4 * H
        quiet = stop - 100 * M
        cloud.add(ID_A, every(T0 - 3 * H, T0 + 9 * H))
        cloud.add(ID_B, every(T0 - 3 * H, quiet, 5 * M))
        while self.clock.t < stop:
            self.cycle(d)
        d._save_state()                                   # stopped
        self.clock.advance(2 * H)
        cloud.add(ID_B, every(quiet + 5 * M, T0 + 9 * H, 5 * M))
        d = self.restart(cloud)
        end = self.clock.t + 6 * H
        while self.clock.t < end:
            cloud.add(ID_A, [self.clock.t])
            cloud.add(ID_B, [self.clock.t])
            self.cycle(d)
        self.assertEqual(self.missing(cloud, ID_B, quiet, stop), [])
        self.assertEqual(self.missing(cloud, ID_A, T0, end - H), [])
        self.assertEqual(len(cloud.sample_calls('recheck')), 1)

    def test_an_ht1_probe_not_seen_live_is_checked_like_any_such_data(self):
        # The daemon pauses 55 min, the cloud stays complete.  The HT1's own
        # record skips 50 min (unchanged readings), from before the restart
        # window's start: its probe stretch holds no sample.  Like any HT1
        # data not seen live, it is checked against 90 min, not 20 - so the
        # skip is no gap, and the probe is read once (corr-r4, compat-r2).
        cloud = ChunkCloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1'})
        cloud.add(ID_A, every(T0 - 3 * H, T0 + 6 * H))
        cloud.add(ID_B, [t for t in every(T0 - 3 * H, T0 + 6 * H, 5 * M)
                         if not T0 + 110 * M < t < T0 + 160 * M])
        d = self.start(cloud)
        while self.clock.t <= T0 + 2 * H + 5 * M:
            self.cycle(d)
        self.assertEqual(d._recheck_last_seen[ID_B], T0 + 110 * M)
        d._save_state()
        self.clock.t = T0 + 3 * H + 5 * M                 # down 55 min
        d = self.restart(cloud)
        self.cycle(d)
        probe = [h for h in d._recheck_holes if h['sid'] == ID_B]
        self.assertEqual([(h['start'], h['end'], h['probe']) for h in probe],
                         [(T0 + 110 * M, T0 + 160 * M, 90 * M)])
        while self.clock.t < T0 + 6 * H:
            self.cycle(d)
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(self.logged(logging.INFO, 'Gap in'), [])
        self.assertEqual(len(cloud.sample_calls('recheck')), 1)
        for sid in (ID_A, ID_B):
            self.assertEqual(self.missing(cloud, sid, T0, T0 + 5 * H), [], sid)

    def test_daemon_paused_55_min_with_the_cloud_complete(self):
        # The poll after the restart starts an hour back, at 2h05: the API
        # leaves out the 2h00 chunk, so 2h06-2h14 would never be stored.
        cloud = ChunkCloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1'})
        cloud.add(ID_A, every(T0 - 3 * H, T0 + 6 * H))
        cloud.add(ID_B, every(T0 - 3 * H, T0 + 6 * H, 5 * M))
        d = self.start(cloud)
        while self.clock.t <= T0 + 2 * H + 5 * M:
            self.cycle(d)
        d._save_state()
        self.clock.t = T0 + 3 * H + 5 * M                 # down 55 min
        d = self.restart(cloud)
        self.cycle(d)
        self.assertEqual(len(self.missing(cloud, ID_A, T0 + H, T0 + 3 * H)), 9)
        self.assertEqual(sorted(h['sid'] for h in d._recheck_holes), [ID_A, ID_B])
        self.assertTrue(all(h['probe'] for h in d._recheck_holes))
        while self.clock.t < T0 + 6 * H:
            self.cycle(d)
        for sid in (ID_A, ID_B):
            self.assertEqual(self.missing(cloud, sid, T0, T0 + 5 * H), [], sid)
        self.assertEqual(len(cloud.sample_calls('recheck')), 1)
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(self.logged(logging.INFO, 'Gap in'), [])

    def test_steady_polling_and_catch_ups_cost_nothing(self):
        # Every 5 minutes for 6 h, then the daemon is down 18 h (a catch-up
        # of two windows) with the cloud complete: no hole, no probe, no
        # re-read call.
        cloud = ChunkCloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1', ID_C: 'HTP.xw'})
        # (HT1 live copies are at most ~13 min apart.)
        for sid, step in ((ID_A, M), (ID_B, 10 * M), (ID_C, M)):
            cloud.add(sid, every(T0 - 3 * H, T0 + D + 6 * H, step))
        d = self.start(cloud)
        while self.clock.t < T0 + 6 * H:
            self.cycle(d)
        d._save_state()
        self.clock.advance(18 * H)
        d = self.restart(cloud)
        while self.clock.t < T0 + D + 4 * H:
            self.cycle(d)
        self.assertEqual(cloud.sample_calls('recheck'), [])
        self.assertEqual(self.logged(logging.INFO, 'Gap in'), [])
        self.assertEqual(self.logged(logging.DEBUG, 'right after the newest sample'), [])
        for sid in (ID_A, ID_B, ID_C):
            self.assertEqual(self.missing(cloud, sid, T0, T0 + D + 3 * H), [], sid)

    def test_hourly_polling(self):
        # INTERVAL = 1h, and a cycle takes 20 s, so polls start 1 h 20 s
        # apart.  With the default DEEP_SCAN_INTERVAL of 1h every cycle is
        # a deep scan, whose window starts an hour before the newest stored
        # point: nothing is unread, no call.  With 6h the polls between
        # start 2 min before the previous one ended, in the middle of a
        # chunk, so the API leaves out up to 13 minutes after it: one probe
        # per sensor and poll, read together an hour later - nothing is
        # lost, at no more than one call per poll.
        for deep, most in (('1h', 0), ('6h', None)):
            with self.subTest(deep_scan_interval=deep):
                self.reset_state()
                del self.records[:]
                cloud = ChunkCloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1'})
                cloud.add(ID_A, every(T0 - 3 * H, T0 + 2 * D))
                cloud.add(ID_B, every(T0 - 3 * H, T0 + 2 * D, 5 * M))
                self.clock.t = T0 + 7 * M
                d = self.start(cloud, interval=3600, deep_scan_interval=deep)
                polls = 0
                while self.clock.t < T0 + D:
                    self.cycle(d, step=H + 20)
                    polls += 1
                for sid in (ID_A, ID_B):
                    self.assertEqual(self.missing(cloud, sid, T0 + H, T0 + D - 2 * H),
                                     [], sid)
                calls = len(cloud.sample_calls('recheck'))
                self.assertLessEqual(calls, polls if most is None else most)
                if most is None:
                    self.assertGreater(calls, polls // 2)
                self.assertLessEqual(len(d._recheck_holes), 2)
                self.assertEqual(self.logged(logging.WARNING, 'more than'), [])
                self.assertEqual(self.logged(logging.INFO, 'Gap in'), [])

    def test_reads_of_what_the_polls_missed_log_no_gap(self):
        # INTERVAL 55 min, a normal day.  A poll between deep scans starts 5
        # min before the newest sample seen, in the middle of a chunk, and
        # the API leaves out what follows up to the next chunk: a probe per
        # sensor and poll, read an hour later, which writes what the poll
        # missed.  None of that is a gap.  It logged 'Re-reading N gap(s)'
        # and a 'Re-read 1/5 of the gap' line per sensor, at INFO; now a
        # call adds one INFO line at most, and only when it found samples,
        # besides the backend's own write line (compat-r2-1).
        cloud = ChunkCloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1', ID_C: 'HTP.xw'})
        for sid, step in ((ID_A, M), (ID_B, 5 * M), (ID_C, M)):
            cloud.add(sid, every(T0 - 3 * H, T0 + 2 * D, step))
        d = self.start(cloud, interval=3300)
        spans = []
        inner = d._recheck_gaps

        def recording(*a, **k):
            first, calls = len(self.records), len(cloud.sample_calls('recheck'))
            try:
                return inner(*a, **k)
            finally:
                spans.append((self.records[first:],
                              len(cloud.sample_calls('recheck')) - calls))
        d._recheck_gaps = recording
        while self.clock.t < T0 + D:
            self.cycle(d, step=55 * M + 20)
        self.assertGreater(sum(n for _, n in spans), 10)
        for records, calls in spans:
            info = [r.getMessage() for r in records if r.levelno == logging.INFO
                    and not r.getMessage().startswith('Write to ')]
            self.assertLessEqual(len(info), calls, info)
            for line in info:
                self.assertNotIn('gap', line.lower())
        self.assertTrue(self.logged(logging.INFO, 'Read again what the polls may have missed'))
        self.assertEqual(self.logged(logging.INFO, 'Gap in'), [])
        for sid in (ID_A, ID_B, ID_C):
            self.assertEqual(self.missing(cloud, sid, T0 + H, T0 + D - 2 * H), [], sid)


# -----------------------------------------------------------------------------
# Isolation
# -----------------------------------------------------------------------------
class Isolation(RecheckCase):

    def test_detection_error_leaves_the_window_written_and_not_failed(self):
        cloud = Cloud(self.clock, {ID_A: 'HT.w'})
        cloud.add(ID_A, every(self.clock.t - 2 * H, self.clock.t))
        w = fake_writer()
        d = self.daemon(cloud, writer=w)
        d._note_fetched = mock.Mock(side_effect=RuntimeError('boom'))
        d._collect_cycle()
        self.assertEqual(len(cloud.sample_calls()), 1)    # not fetched again
        self.assertEqual(d._note_fetched.call_count, 1)
        self.assertEqual(len(written_times(w)), 61)
        self.assertEqual(d._failed_windows, [])
        self.assertIsNotNone(d._fetch_high_water)
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap detection failed')), 1)

    def test_detection_errors_warn_once_per_cycle(self):
        cloud = Cloud(self.clock, {ID_A: 'HT.w'})
        cloud.add(ID_A, every(self.clock.t - 3 * D, self.clock.t, 5 * M))
        w = fake_writer(newest={TAG_A: at(self.clock.t - 2 * D)})
        d = self.daemon(cloud, writer=w)
        d._note_fetched = mock.Mock(side_effect=RuntimeError('boom'))
        d._collect_cycle()                       # a catch-up of several windows
        self.assertGreater(d._note_fetched.call_count, 3)
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap detection failed')), 1)
        self.clock.advance(5 * M)
        d._collect_cycle()
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap detection failed')), 2)
        self.assertEqual(d._failed_windows, [])

    def test_recheck_error_does_not_fail_the_cycle(self):
        cloud = Cloud(self.clock, {ID_A: 'HT.w'})
        cloud.add(ID_A, every(self.clock.t - 2 * H, self.clock.t))
        d = self.daemon(cloud)
        d._recheck_gaps = mock.Mock(side_effect=RuntimeError('boom'))
        d._collect_cycle()
        d._recheck_gaps.assert_called_once()
        self.assertEqual(len(self.logged(logging.ERROR, 'Gap re-check failed; '
                                                        'collection is not affected')), 1)
        state = json.loads((self.state_dir / 'state.json').read_text())
        self.assertEqual(datetime.datetime.fromisoformat(state['collected_until']),
                         d._fetch_high_water)

    def test_failed_payload_still_saves_the_other_keys(self):
        d = self.daemon(Cloud(self.clock, {}))
        d._collected_until = at(self.clock.t)
        d._recheck_payload = mock.Mock(side_effect=RuntimeError('boom'))
        d._save_state()
        state = json.loads((self.state_dir / 'state.json').read_text())
        self.assertEqual(list(state), ['sensors', 'failed_windows', 'collected_until'])
        self.assertEqual(len(self.logged(logging.WARNING, 'Could not save the gap '
                                                          're-checks')), 1)

    def test_no_rechecks_after_a_main_fetch_that_failed(self):
        cloud = Cloud(self.clock, {ID_A: 'HT.w'})
        d = self.daemon(cloud)
        d._recheck_gaps = mock.Mock()
        cloud.fail = 3                           # every attempt of the window
        d._collect_cycle()
        self.assertEqual(len(d._failed_windows), 1)
        d._recheck_gaps.assert_not_called()

    def test_malformed_entries_are_dropped_and_nothing_else_is_touched(self):
        now = self.clock.t
        good = {'sid': ID_A, 'start': now - 3 * H, 'end': now - 2 * H,
                'closed_at': now - 2 * H, 'done': 1, 'pos': 0, 'cell': 660,
                'counts': [42]}
        until = at(now - H)
        state = self.write_state(
            [good, dict(good, start='x'), dict(good, done=-1), dict(good, pos=1.5),
             dict(good, cell=10), dict(good, sid=None), 'junk',
             dict(good, end=now + 60 * D),
             dict(good, counts=[1, 2, 3])],       # wrong length: reset, kept
            sensors={ID_B: {'gap_from': at(now - 9 * H).isoformat(),
                            'stale_since': now - 2 * H}},
            failed_windows=[['2026-11-08T01:00:00+0100', '2026-11-08T13:00:00+0100']],
            collected_until=until.isoformat())
        state['rechecks']['last_seen'] = {ID_A: now - 60, ID_B: 'yesterday'}
        (self.state_dir / 'state.json').write_text(json.dumps(state))
        before = (self.state_dir / 'state.json').read_text()
        # A marker the current format must never adopt.
        (self.state_dir / 'last-success').write_text(at(now).isoformat() + '\n')
        d = self.daemon(Cloud(self.clock, {ID_A: 'HT1'}))
        self.assertEqual(d._sensor_state, state['sensors'])
        self.assertEqual(d._failed_windows, state['failed_windows'])
        self.assertEqual(d._collected_until, until)
        self.assertEqual(d._recheck_last_seen, {ID_A: now - 60})
        # A file without the probe key (all of them so far) means a gap.
        self.assertEqual(d._recheck_holes, [dict(good, probe=None),
                                            dict(good, counts=[0], probe=None)])
        warnings = self.logged(logging.WARNING)
        self.assertEqual(len(warnings), 1)
        self.assertIn('Dropped 8 malformed gap re-check entries', warnings[0])
        self.assertEqual((self.state_dir / 'state.json').read_text(), before)

    def test_values_the_daemon_never_writes_are_dropped_one_by_one(self):
        # A damaged or hand-edited file: each bad entry is dropped alone.
        now = self.clock.t
        good = {'sid': ID_A, 'start': now - 3 * H, 'end': now - 2 * H,
                'closed_at': now - 2 * H, 'done': 0, 'pos': 0, 'cell': 660,
                'counts': [0]}
        bad = [dict(good, start=1e12, end=1e12 + H),          # year 33658
               dict(good, start=-7e10, end=-7e10 + H),        # before year 1
               dict(good, start=1e9, end=1e9 + H),            # 2001
               dict(good, start=10 ** 400), dict(good, cell=10 ** 400),
               dict(good, closed_at=7.3e9),                   # never due
               dict(good, end=now + 2 * D),
               # Longer than on-board storage plus a day: never registered.
               dict(good, start=now - 48 * D, end=now - D + H),
               dict(good, probe='yes'), dict(good, probe=-1.0)]
        state = self.write_state([good] + bad)
        state['rechecks']['last_seen'] = {ID_A: now - 60, ID_B: now + 365 * D,
                                          ID_C: 10 ** 400}
        (self.state_dir / 'state.json').write_text(json.dumps(state))
        d = self.daemon(Cloud(self.clock, {ID_A: 'HT.w'}))
        self.assertEqual(d._recheck_holes, [dict(good, probe=None)])
        self.assertEqual(d._recheck_last_seen, {ID_A: now - 60})
        warnings = self.logged(logging.WARNING)
        self.assertEqual(len(warnings), 1)
        self.assertIn('Dropped %d malformed' % (len(bad) + 2), warnings[0])

    def test_one_broken_hole_does_not_stop_the_others(self):
        # Holes whose bounds the date functions cannot take - finished, or
        # due - neither raise out of the re-checks nor keep the others
        # from being read.
        cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT.w'})
        d = self.daemon(cloud)
        sensors = cloud.sensors()
        t0 = self.clock.t
        cloud.add(ID_B, every(t0 - 6 * H, t0 - 3 * H) + every(t0 - 2 * H, t0))
        d._register_hole(ID_B, t0 - 3 * H, t0 - 2 * H, sensors, now=t0)
        good = d._recheck_holes[0]
        d._recheck_holes.insert(0, dict(good, sid=ID_A, start=-7e10,
                                        end=-7e10 + H, done=5))
        d._recheck_holes.insert(0, dict(good, sid=ID_A, start=1e12,
                                        end=1e12 + H, counts=[0]))
        self.clock.advance(H)
        for _ in range(4):
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(20 * M)
        self.assertEqual(good['done'], 1)
        self.assertNotIn(-7e10, [h['start'] for h in d._recheck_holes])
        self.assertEqual(self.logged(logging.ERROR), [])

    def test_unreadable_rechecks_key_leaves_the_other_keys_alone(self):
        until = at(self.clock.t - H)
        self.write_state([], collected_until=until.isoformat(),
                         failed_windows=[['a', 'b']])
        state = json.loads((self.state_dir / 'state.json').read_text())
        state['rechecks'] = 'garbage'
        (self.state_dir / 'state.json').write_text(json.dumps(state))
        d = self.daemon(Cloud(self.clock, {}))
        self.assertEqual((d._collected_until, d._failed_windows), (until, [['a', 'b']]))
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(len(self.logged(logging.WARNING, 'Could not read the gap '
                                                          're-checks')), 1)


# -----------------------------------------------------------------------------
# Holes
# -----------------------------------------------------------------------------
class Holes(RecheckCase):

    UNKNOWN = '345678.9012345678901234'

    def setUp(self):
        super().setUp()
        self.cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HTP.xw', ID_C: 'HT.w',
                                        self.UNKNOWN: 'HTX.future'})
        self.sensors = self.cloud.sensors()
        self.d = self.daemon(self.cloud)

    def test_unread_holes_close_together_are_merged(self):
        now = self.clock.t
        self.d._register_hole(ID_A, now - 12 * H, now - 11 * H, self.sensors)
        first = self.d._recheck_holes[0]
        self.clock.advance(10 * M)
        # Starts within RECHECK_MERGE_WITHIN of the first one's end.
        self.d._register_hole(ID_A, now - 5 * H, now - 2 * H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 1)
        self.assertEqual((first['start'], first['end'], first['closed_at']),
                         (now - 12 * H, now - 2 * H, now))
        self.assertEqual(first['counts'], [0, 0])     # 10 h + margins: new cells
        self.assertEqual(len(self.logged(logging.INFO, "Gap in 'S0' extended")), 1)
        # Further away: a hole of its own.
        self.d._register_hole(ID_A, now + 5 * H, now + 6 * H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 2)
        # A hole whose re-reads have begun does not grow.
        first['done'] = 1
        second = self.d._recheck_holes[1]
        second['pos'] = 1
        self.d._register_hole(ID_A, now - H, now, self.sensors)
        self.d._register_hole(ID_A, now + 7 * H, now + 8 * H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 4)
        # Nor does one of another sensor.
        self.d._register_hole(ID_C, now + 7 * H, now + 8 * H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 5)

    def test_per_sensor_cap_merges_the_closest_holes(self):
        now = self.clock.t
        starts = [now - 15 * D + k * 8 * H for k in range(40)]
        for s in starts:
            self.d._register_hole(ID_A, s, s + H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 40)
        for hole in self.d._recheck_holes:
            hole['done'] = 1                    # read once: the merge rule is out
        self.clock.advance(H)
        self.d._register_hole(ID_A, starts[9] + 2 * H, starts[9] + 3 * H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 40)
        merged = [h for h in self.d._recheck_holes if h['start'] == starts[9]][0]
        self.assertEqual((merged['end'], merged['closed_at'], merged['done'],
                          merged['pos'], merged['counts']),
                         (starts[9] + 3 * H, now + H, 0, 0, [0]))
        self.assertEqual(len(self.logged(logging.WARNING, 'more than 40 gaps')), 1)
        # Once a day at most.
        self.d._register_hole(ID_A, starts[20] + 2 * H, starts[20] + 3 * H, self.sensors)
        self.assertEqual(len(self.logged(logging.WARNING, 'more than 40 gaps')), 1)
        self.clock.advance(D)
        self.d._register_hole(ID_A, starts[30] + 2 * H, starts[30] + 3 * H, self.sensors)
        self.assertEqual(len(self.logged(logging.WARNING, 'more than 40 gaps')), 2)
        self.assertEqual(len(self.d._recheck_holes), 40)
        # Other sensors are not affected.
        self.d._register_hole(ID_C, now, now + H, self.sensors)
        self.assertEqual(len(self.d._recheck_holes), 41)

    def test_merged_probes_stay_a_probe_unless_merged_with_a_gap(self):
        # While re-reads wait (say, a backend failing for a day), polls 45
        # min apart add a probe per sensor and poll, and at the cap the
        # closest are merged.  Two probes merged are a probe, checked against
        # the stricter threshold; only a gap merged in makes a gap (compat-r2).
        now = self.clock.t
        with mock.patch.object(spd, 'RECHECK_HOLES_PER_SENSOR', 2):
            for k, probe in enumerate((90 * M, 20 * M)):
                self.d._register_hole(ID_A, now - 10 * H + k * 40 * M,
                                      now - 10 * H + k * 40 * M + 10 * M,
                                      self.sensors, probe=probe)
            self.d._register_hole(ID_A, now - 2 * H, now - 2 * H + 5 * M,
                                  self.sensors, probe=90 * M)
            holes = sorted(self.d._recheck_holes, key=lambda h: h['start'])
            self.assertEqual([(h['start'], h['end'], h['probe']) for h in holes],
                             [(now - 10 * H, now - 10 * H + 50 * M, 20 * M),
                              (now - 2 * H, now - 2 * H + 5 * M, 90 * M)])
            self.d._register_hole(ID_A, now - 2 * H + 30 * M, now - H, self.sensors)
            holes = sorted(self.d._recheck_holes, key=lambda h: h['start'])
            self.assertEqual([(h['start'], h['end'], h['probe']) for h in holes],
                             [(now - 10 * H, now - 10 * H + 50 * M, 20 * M),
                              (now - 2 * H, now - H, None)])
        # A merged probe is read once: dropped if no step in it is longer
        # than its threshold, read on as a gap otherwise.
        for step, left in ((15 * M, []), (25 * M, [(1, None)])):
            with self.subTest(step=step):
                self.reset_state()
                cloud = Cloud(self.clock, {ID_A: 'HT.w'})
                d = self.daemon(cloud)
                t0 = self.clock.t
                with mock.patch.object(spd, 'RECHECK_HOLES_PER_SENSOR', 1):
                    d._register_hole(ID_A, t0 - 5 * H, t0 - 5 * H + 10 * M,
                                     cloud.sensors(), now=t0, probe=20 * M)
                    d._register_hole(ID_A, t0 - 4 * H, t0 - 4 * H + 10 * M,
                                     cloud.sensors(), now=t0, probe=20 * M)
                cloud.add(ID_A, every(t0 - 6 * H, t0 - 5 * H)
                          + every(t0 - 5 * H + step, t0, step))
                self.clock.advance(H)
                d._recheck_gaps(cloud.sensors(), 'SensorPush')
                self.assertEqual([(h['done'], h['probe']) for h in d._recheck_holes],
                                 left)

    def test_at_most_1000_holes_in_all(self):
        # 100 sensors flapping would keep 4,000 holes and rewrite ~0.7 MiB of
        # state.json every cycle (load-6).  Beyond 1,000 the closest holes of
        # the sensor with the most are merged; every stretch stays covered,
        # and probes merged with probes stay probes.
        sids = ['%d.%016d' % (500000 + k, k) for k in range(30)]
        cloud = Cloud(self.clock, {sid: 'HT.w' for sid in sids})
        sensors = cloud.sensors()
        d = self.daemon(cloud)
        now = self.clock.t
        registered = []
        for k in range(34):                     # 30 x 34 = 1,020 holes
            for i, sid in enumerate(sids):
                # 6 h 40 min apart (not merged as they come), but the
                # probes in pairs, 5 h 40 min apart: those are the closest.
                a = now - 20 * D + k * 7 * H - (H if k % 4 == 2 else 0) + i * 60
                probe = 20 * M if k % 4 in (1, 2) else None
                d._register_hole(sid, a, a + 20 * M, sensors, now=now, probe=probe)
                registered.append((sid, a, a + 20 * M, probe))
        self.assertEqual(len(d._recheck_holes), 1000)
        merged = [h for h in d._recheck_holes if h['end'] - h['start'] > 20 * M]
        self.assertEqual(len(merged), 20)
        self.assertEqual({h['probe'] for h in merged}, {20 * M})
        for sid, a, b, probe in registered:
            cover = [h for h in d._recheck_holes
                     if h['sid'] == sid and h['start'] <= a and b <= h['end']]
            self.assertEqual(len(cover), 1, (sid, a))
            if probe and cover[0]['end'] - cover[0]['start'] > 20 * M:
                # Merged: a probe only if all it holds came from probes.
                parts = [p for s, x, y, p in registered if s == sid
                         and cover[0]['start'] <= x and y <= cover[0]['end']]
                self.assertEqual(cover[0]['probe'],
                                 20 * M if all(parts) else None, (sid, a))
        self.assertEqual(len(self.logged(logging.WARNING, 'More than 1000 gaps')), 1)

    def test_merges_never_cut_a_hole_short(self):
        # Two HT1 holes 30 days apart: merged, the hole would be clamped to
        # the 20 days on board the later one ends with, and the earlier one,
        # whose data may still be in the cloud, never read again.  They are
        # kept apart, even beyond a cap.
        now = self.clock.t
        with mock.patch.object(spd, 'RECHECK_HOLES_PER_SENSOR', 1):
            self.d._register_hole(ID_A, now - 31 * D, now - 30 * D, self.sensors)
            self.d._register_hole(ID_A, now - D, now - D + H, self.sensors)
        self.assertEqual(sorted((h['start'], h['end']) for h in self.d._recheck_holes),
                         [(now - 31 * D, now - 30 * D), (now - D, now - D + H)])

    def test_storage_clamp_per_type(self):
        now = self.clock.t
        cell = 660 * M
        for sid, days in ((ID_A, 20), (ID_B, 30), (ID_C, 45), (self.UNKNOWN, 45)):
            self.d._register_hole(sid, now - 60 * D, now, self.sensors)
            start = self.d._recheck_holes[-1]['start']
            # Moved forward by whole cells, keeping a little more than the
            # storage: the cells stay on the grid of the unclamped holes.
            earliest = now - (days + 1) * D
            self.assertTrue(earliest - cell < start <= earliest, sid)
            self.assertEqual((start - (now - 60 * D)) % cell, 0, sid)
        self.assertEqual(len(self.logged(logging.INFO, "on-board storage")), 4)
        # A 21-day HT1 hole is the 47-cell worst case.
        self.assertEqual(spd._hole_ncells(self.d._recheck_holes[0]), 47)
        self.d._register_hole(ID_A, now + 2 * D, now + 3 * D, self.sensors)
        self.assertEqual(self.d._recheck_holes[-1]['start'], now + 2 * D)
        self.assertEqual(len(self.logged(logging.INFO, "on-board storage")), 4)

    def test_clamped_cells_line_up_with_the_other_sensors_of_the_outage(self):
        # A 30-day outage of the whole installation: the HT1 hole is clamped
        # to its 20 days, the HTP.xw and HT.w holes are not.  On the shared
        # grid every call for an HT1 cell serves the other two as well.
        t0 = self.clock.t
        a, b = t0 - 30 * D, t0 - H
        sensors = {sid: self.sensors[sid] for sid in (ID_A, ID_B, ID_C)}
        for k, sid in enumerate((ID_A, ID_B, ID_C)):
            self.d._register_hole(sid, a + 7 * k, b, sensors, now=t0)
        holes = {h['sid']: h for h in self.d._recheck_holes}
        self.assertEqual(((holes[ID_A]['start'] - a) % (660 * M)), 0)
        self.clock.t = t0 + H
        before = len(self.cloud.sample_calls())
        while holes[ID_A] in self.d._recheck_holes and holes[ID_A]['done'] == 0:
            self.d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        calls = self.cloud.sample_calls()[before:]
        with_ht1 = [c for c in calls if ID_A in c[4]]
        self.assertEqual(len(with_ht1), spd._hole_ncells(holes[ID_A]))
        self.assertTrue(all(set(c[4]) == {ID_A, ID_B, ID_C} for c in with_ht1))
        self.assertEqual(len(calls), spd._hole_ncells(holes[ID_C]))


# -----------------------------------------------------------------------------
# Scheduling
# -----------------------------------------------------------------------------
class Scheduling(RecheckCase):

    def setUp(self):
        super().setUp()
        self.cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT1', ID_C: 'HT.w'})
        self.sensors = self.cloud.sensors()
        self.d = self.daemon(self.cloud)

    def hole(self, sid, start, end, closed_at):
        self.d._register_hole(sid, start, end, self.sensors, now=closed_at)
        return self.d._recheck_holes[-1]

    def rechecks(self, d=None):
        """Run the re-checks once; the get_samples calls they made."""
        before = len(self.cloud.calls)
        (d or self.d)._recheck_gaps(self.sensors, 'SensorPush')
        return [c for c in self.cloud.calls[before:] if c[0] == 'get_samples']

    def test_due_times(self):
        t0 = self.clock.t
        hole = self.hole(ID_A, t0 - 3 * H, t0 - 2 * H, t0)
        for done, offset in enumerate(OFFSETS, 1):
            self.clock.t = t0 + offset - 60
            self.assertEqual(self.rechecks(), [], offset)
            self.clock.t = t0 + offset
            calls = self.rechecks()
            self.assertEqual(len(calls), 1, offset)
            self.assertEqual(calls[0][4], (ID_A,))
            self.assertEqual(hole['done'], done)
        self.assertEqual(self.d._recheck_holes, [])

    def test_early_holes_only_ride_along_with_a_due_one(self):
        t0 = self.clock.t
        self.assertEqual(self.d._early_allowance(0), 15 * M)
        self.assertEqual(self.d._early_allowance(4), 30 * M)    # keeps the horizon
        b = self.hole(ID_B, t0 - 3 * H, t0 - 2 * H, t0 + 10 * M)   # due at +1h10m
        far = self.hole(ID_C, t0 - 3 * D, t0 - 3 * D + H, t0 + 10 * M)
        self.clock.t = t0 + H
        self.assertEqual(self.rechecks(), [])    # almost due, but nothing is due
        a = self.hole(ID_A, t0 - 3 * H - 5 * M, t0 - 2 * H, t0)   # due now
        calls = self.rechecks()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][4], tuple(sorted((ID_A, ID_B))))
        self.assertEqual((a['done'], b['done'], far['done']), (1, 1, 0))

    def test_offsets_missed_while_down_collapse_into_one_read(self):
        t0 = self.clock.t
        hole = self.hole(ID_A, t0 - 3 * H, t0 - 2 * H, t0)
        self.clock.t = t0 + 4 * D               # 1h, 6h, 1d and 3d went by
        self.assertEqual(len(self.rechecks()), 1)
        self.assertEqual(hole['done'], 4)
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read 4/5')), 1)
        self.clock.t = t0 + 7 * D - 31 * M
        self.assertEqual(self.rechecks(), [])
        self.clock.t = t0 + 7 * D
        self.assertEqual(len(self.rechecks()), 1)
        self.assertEqual(self.d._recheck_holes, [])

    def test_a_shortened_schedule_finishes_holes_beyond_it(self):
        t0 = self.clock.t
        self.write_state([{'sid': ID_A, 'start': t0 - 3 * H, 'end': t0 - 2 * H,
                           'closed_at': t0 - 4 * D, 'done': 3, 'pos': 0,
                           'cell': 660, 'counts': [7]}])
        d = self.daemon(self.cloud, recheck_gaps='1h, 6h')
        self.assertEqual(len(d._recheck_holes), 1)
        self.assertEqual(self.rechecks(d), [])
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(len(self.logged(
            logging.INFO, 're-checks finished; the cloud holds 7 samples in it')), 1)

    def test_removal_and_the_summary_line(self):
        t0 = self.clock.t
        a, b = t0 - 3 * H, t0 - 2 * H
        self.cloud.add(ID_A, every(a - H, a, 10 * M) + every(b, t0, 10 * M))
        self.hole(ID_A, a, b, t0)
        self.clock.t = t0 + H
        self.rechecks()
        late = every(a + 10 * M, b - 10 * M, 2 * M)     # 21 arrive late
        self.cloud.add(ID_A, late)
        for offset in OFFSETS[1:]:
            self.clock.t = t0 + offset
            self.rechecks()
        self.assertEqual(self.d._recheck_holes, [])
        lines = self.logged(logging.INFO, 'Re-read ')
        self.assertEqual(len(lines), 5)
        self.assertIn('0 samples, nothing new', lines[0])
        self.assertEqual(lines[1], "Re-read 2/5 of the gap in 'S0' (%s): 21 samples, "
                                   "21 new, written" % spd._fmt_span(a, b, t0 + 6 * H))
        self.assertIn('Re-read 5/5', lines[4])
        self.assertIn('21 samples, nothing new', lines[4])
        self.assertEqual(self.logged(logging.INFO, 're-checks finished'),
                         ["Gap in 'S0' %s: re-checks finished; the cloud holds %d "
                          "samples in it" % (spd._fmt_span(a, b, t0 + 7 * D), len(late))])

    def test_a_schedule_without_a_3d_read_keeps_every_read(self):
        # RECHECK_GAPS = 1h, 6h, 1d has no later read to skip ahead to, so a
        # long gap that is still empty at the first read is read at 6h and
        # 1d as well: the HT1's history, which comes 5 h late, is caught.
        for schedule in ('1h, 6h, 1d', '1h, 12h, 2d'):
            with self.subTest(schedule):
                self.reset_state()
                cloud = Cloud(self.clock, {ID_A: 'HT1'})
                w = fake_writer()
                d = self.daemon(cloud, writer=w, recheck_gaps=schedule)
                t0 = self.clock.t
                a, b = t0 - 2 * D, t0
                cloud.add(ID_A, every(a - H, a, 5 * M) + every(b, b + 9 * D, 5 * M))
                d._register_hole(ID_A, a, b, cloud.sensors(), now=t0)
                history = every(a + 5 * M, b - 5 * M, 5 * M)
                for k in range(3 * 24):              # hourly, for 3 days
                    self.clock.t = t0 + (k + 1) * H
                    if self.clock.t == t0 + 5 * H:
                        cloud.add(ID_A, history)
                    d._recheck_gaps(cloud.sensors(), 'SensorPush')
                self.assertTrue(set(history) <= set(written_times(w)))
                self.assertEqual(d._recheck_holes, [])
                self.assertEqual(self.logged(logging.INFO, 'next re-read after'), [])

    def test_holes_whose_cells_start_a_little_earlier_share_the_call(self):
        # The sensors of one outage come back over a few polls, so the hole
        # due first need not be the one whose cell starts first: the call
        # window begins at the earliest cell up to 30 min before.
        t0 = self.clock.t
        self.hole(ID_C, t0 - 3 * H + 11, t0 - 30 * M, t0)              # the anchor
        self.hole(ID_B, t0 - 3 * H, t0 - 25 * M, t0 + 5 * M)          # 11 s earlier
        self.hole(ID_A, t0 - 3 * H - 12 * M, t0 - 20 * M, t0 + 10 * M)  # 12 min
        self.hole(ID_A, t0 - 4 * H, t0 - 3 * H - 40 * M, t0 + 10 * M)   # too early
        self.clock.t = t0 + H + 10 * M
        calls = self.rechecks()
        self.assertEqual(len(calls), 2)
        self.assertEqual(set(calls[0][4]), {ID_A, ID_B, ID_C})
        start = spd._observed_epoch(calls[0][2])
        self.assertEqual(start, t0 - 3 * H - 12 * M - H - 30 * M)
        # It ends half an hour after the last cell it serves (ID_A's).
        self.assertEqual(spd._observed_epoch(calls[0][3]), t0 - 20 * M + H + 30 * M)
        self.assertEqual([h['done'] for h in self.d._recheck_holes], [1] * 4)

    def test_a_call_ends_half_an_hour_after_the_last_cell_it_serves(self):
        # A 40-min silence: its cell, with the hour either side, is 2 h 40
        # min long, and the call reads it plus the half hour on either side
        # that every window has - not a full 12-h window, more than three
        # times as long (load-7).  A long hole's cells are still read whole.
        t0 = self.clock.t
        a, b = t0 - 3 * H, t0 - 3 * H + 40 * M
        self.hole(ID_C, a, b, t0)
        self.hole(ID_A, t0 - 3 * D, t0 - 2 * D, t0)      # 3 cells of 11 h
        self.clock.t = t0 + H
        calls = [(spd._observed_epoch(c[2]), spd._observed_epoch(c[3]))
                 for c in self.rechecks()]
        c0 = t0 - 3 * D - H
        self.assertEqual(calls, [(c0 - 30 * M, c0 + 11 * H + 30 * M),
                                 (c0 + 11 * H - 30 * M, c0 + 22 * H + 30 * M),
                                 (c0 + 22 * H - 30 * M, t0 - 2 * D + H + 30 * M),
                                 (a - H - 30 * M, b + H + 30 * M)])

    def test_holes_of_sensors_no_longer_polled(self):
        t0 = self.clock.t
        self.hole(ID_A, t0 - 3 * H, t0 - 2 * H, t0)
        others = {ID_B: self.sensors[ID_B]}
        self.clock.t = t0 + H
        self.d._recheck_gaps(others, 'SensorPush')
        self.assertEqual(self.cloud.sample_calls(), [])
        self.assertEqual(len(self.d._recheck_holes), 1)    # kept for a while
        self.clock.t = t0 + 8 * D + 1
        self.d._recheck_gaps(others, 'SensorPush')
        self.assertEqual(self.d._recheck_holes, [])
        self.assertEqual(len(self.logged(logging.INFO, 'no longer polled')), 1)
        # --sensorlist counts the same way.
        d = self.daemon(self.cloud, args=make_args(sensorlist=[ID_B]))
        d._register_hole(ID_A, t0, t0 + H, self.sensors, now=self.clock.t)
        self.clock.advance(H)
        self.assertEqual(self.rechecks(d), [])


# -----------------------------------------------------------------------------
# Reading and writing
# -----------------------------------------------------------------------------
class ReadingAndWriting(RecheckCase):

    def setUp(self):
        super().setUp()
        self.cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT.w', ID_C: 'HTP.xw'})
        self.sensors = self.cloud.sensors()
        self.w = fake_writer()
        self.d = self.daemon(self.cloud, writer=self.w)

    def rechecks(self, d=None):
        (d or self.d)._recheck_gaps(self.sensors, 'SensorPush')

    def test_only_cells_are_written_margins_included_and_only_when_it_grows(self):
        t0 = self.clock.t
        a, b = t0 - 5 * H, t0 - 4 * H
        self.cloud.add(ID_A, every(t0 - 7 * H, a, 5 * M) + every(b, t0, 5 * M))
        self.d._register_hole(ID_A, a, b, self.sensors, now=t0)
        backlog = every(a + 2 * M, b - 2 * M, 2 * M)
        # Own samples next to the gap often come only with the backlog.
        margins = [a - 50 * M + 30, b + 40 * M + 30]
        # In the call window, but outside the cell: never written.
        outside = [a - 61 * M, b + H + 30]
        self.cloud.add(ID_A, backlog + margins + outside)
        self.clock.t = t0 + H
        self.rechecks()
        cell = sorted(t for t in self.cloud.samples[ID_A] if a - H <= t <= b + H)
        self.assertEqual(written_times(self.w), cell)
        self.assertTrue(set(margins) <= set(cell))
        self.assertEqual(self.d._recheck_holes[0]['counts'], [len(backlog)])
        # Nothing new by the next read: nothing is written.
        self.clock.t = t0 + 6 * H
        self.rechecks()
        self.assertEqual(written_times(self.w), [])
        self.assertEqual(len(self.cloud.sample_calls()), 2)
        # More by the one after: the whole cell again.
        late = [a + 61, a + 3 * M + 1]
        self.cloud.add(ID_A, late)
        self.clock.t = t0 + D
        self.rechecks()
        self.assertEqual(written_times(self.w), sorted(cell + late))
        self.assertEqual(self.d._recheck_holes[0]['counts'], [len(backlog) + 2])

    def test_only_settled_samples_count(self):
        t0 = self.clock.t
        d = self.daemon(self.cloud, writer=self.w, recheck_gaps='30m, 1d')
        a, b = t0 - 40 * M, t0
        self.cloud.add(ID_B, every(a - H, a))
        d._register_hole(ID_B, a, b, self.sensors, now=t0)
        backlog = every(a + M, b - M)
        self.cloud.add(ID_B, backlog + every(b, b + H))
        self.clock.t = t0 + 30 * M
        self.rechecks(d)
        settled = [t for t in backlog if t <= self.clock.t - 35 * M]
        self.assertLess(len(settled), len(backlog))
        hole = d._recheck_holes[0]
        self.assertEqual(hole['counts'], [len(settled)])
        # Complete by then, but too fresh to tell: it is read again.
        self.assertEqual(hole['done'], 1)

    def failing_hole(self):
        t0 = self.clock.t
        a, b = t0 - 5 * H, t0 - 4 * H
        self.cloud.add(ID_A, every(t0 - 7 * H, a, 5 * M) + every(b, t0, 5 * M)
                       + every(a + 5 * M, b - 5 * M, 5 * M))
        self.d._register_hole(ID_A, a, b, self.sensors, now=t0)
        self.clock.advance(H)
        return self.d._recheck_holes[0]

    def test_failed_call_and_failed_write_advance_nothing(self):
        def api_error():
            self.cloud.fail = 1

        def write_error():
            # The backend refuses the re-read records only: the polls land.
            self.w.write = mock.Mock(side_effect=OSError('database down'))

        def persisted(hole):
            return {k: hole[k] for k in spd.HOLE_KEYS}

        def rechecks():
            self.w._consecutive_failures = 0      # the cycle's poll landed
            self.rechecks()

        for inject in (api_error, write_error):
            with self.subTest(inject.__name__):
                self.reset_state()
                self.cloud = Cloud(self.clock, {ID_A: 'HT1'})
                self.w = fake_writer()
                self.d = self.daemon(self.cloud, writer=self.w)
                hole = self.failing_hole()
                fresh = persisted(hole)
                inject()
                rechecks()
                self.assertEqual(len(self.cloud.sample_calls()), 1)
                self.assertEqual(persisted(hole), fresh)
                self.assertEqual(self.d._recheck_fail_streak, 1)
                # The back-off holds re-reads off for one interval ...
                self.clock.advance(4 * M)
                rechecks()
                self.assertEqual(len(self.cloud.sample_calls()), 1)
                # ... and longer after a second failure in a row.
                inject()
                self.clock.advance(2 * M)
                rechecks()
                self.assertEqual(len(self.cloud.sample_calls()), 2)
                self.assertEqual(persisted(hole), fresh)
                self.assertEqual(self.d._recheck_fail_streak, 2)
                self.assertEqual(self.d._recheck_next_at - self.clock.mono, 15 * M)
                # Once it works again, the hole advances.  After two failed
                # calls its cell is read alone; a read alone that works shows
                # that the API and the backend work, so it resets the common
                # streak too (load-r4-4; round 4 left it at 2).
                self.cloud.fail = 0
                self.w.__dict__.pop('write', None)
                self.clock.advance(15 * M)
                rechecks()
                self.assertEqual((hole['done'], self.d._recheck_fail_streak,
                                  self.d._recheck_alone_streak), (1, 0, 0))
                self.assertEqual(self.w._consecutive_failures, 0)
                self.assertNotIn('_fail', hole)
                self.clock.advance(5 * H)              # the 6-h read
                rechecks()
                self.assertEqual((hole['done'], self.d._recheck_fail_streak), (2, 0))

    def test_a_truncated_answer_halves_the_cells_and_loses_nothing(self):
        # -q 400: the 7-h re-read window of a sensor that records every
        # minute holds more.  The cells are halved and read again at once,
        # without a back-off; nothing of the gap is lost.
        cloud = LimitCloud(self.clock, {ID_B: 'HT.w'})
        w = fake_writer()
        d = self.daemon(cloud, writer=w, args=make_args(qlimit=400))
        t0 = self.clock.t
        a, b = t0 - 6 * H, t0 - 2 * H
        cloud.add(ID_B, every(t0 - 12 * H, a) + every(b, t0 + H))
        d._register_hole(ID_B, a, b, self.sensors, now=t0)
        hole = d._recheck_holes[0]
        backlog = every(a + M, b - M)
        cloud.add(ID_B, backlog)
        self.clock.t = t0 + H
        d._recheck_gaps(self.sensors, 'SensorPush')
        self.assertEqual((cloud.truncated_calls, len(cloud.sample_calls())), (1, 3))
        self.assertEqual(d._recheck_fail_streak, 0)
        self.assertIsNone(d._recheck_next_at)
        self.assertEqual(hole['cell'], 330)
        # Read in full in the same cycle - and complete, so it is dropped.
        self.assertEqual(hole['done'], 1)
        self.assertEqual(d._recheck_holes, [])
        self.assertTrue(set(backlog) <= set(written_times(w)))
        warning = self.logged(logging.WARNING, 'was truncated')
        self.assertEqual(len(warning), 1)
        self.assertIn('-q/--qlimit', warning[0])
        self.assertIn('330-min cells', warning[0])

    def test_a_truncated_anchor_holds_up_no_other_hole(self):
        # The HT.w window is truncated (-q 150); the HT1 hole, whose late
        # backlog arrives at +3 h, is read anyway.
        cloud = LimitCloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT1'})
        w = fake_writer()
        d = self.daemon(cloud, writer=w, args=make_args(qlimit=150))
        sensors = cloud.sensors()
        t0 = self.clock.t
        cloud.add(ID_A, every(t0 - 6 * H, t0 - 3 * H) + every(t0 - 2 * H, t0 + 9 * D))
        d._register_hole(ID_A, t0 - 3 * H, t0 - 2 * H, sensors, now=t0)
        cloud.add(ID_B, every(t0 - 20 * H, t0 - 10 * H, 10 * M)
                  + every(t0 - 8 * H, t0 + 9 * D, 10 * M))
        d._register_hole(ID_B, t0 - 10 * H, t0 - 8 * H, sensors, now=t0 + 60)
        late = every(t0 - 10 * H + 10 * M, t0 - 8 * H - 10 * M, 10 * M)
        for _ in range(8 * 24 * 12):             # 8 days of 5-min cycles
            self.clock.advance(5 * M)
            if abs(self.clock.t - (t0 + 3 * H)) < 1:
                cloud.add(ID_B, late)
            d._recheck_gaps(sensors, 'SensorPush')
        self.assertTrue(set(late) <= set(written_times(w)))
        self.assertEqual(d._recheck_holes, [])
        # A few halvings, never a call that failed for good.
        self.assertGreater(cloud.truncated_calls, 0)
        self.assertLess(len(cloud.sample_calls()), 30)
        self.assertEqual(d._recheck_fail_streak, 0)

    def test_truncated_even_in_the_smallest_cells_counts_as_a_failed_call(self):
        # -q 60: even a 90-min window holds more samples of an HT.w.  Such an
        # answer is a failed call (load-r3): re-reads pause, longer each
        # time, instead of spending 6 calls a cycle on cells they cannot
        # read.  The cell is tried again after the pause (the newest live
        # copies may be gone by then) and skipped after 3 failed calls, the
        # hole's next cells after one more each; the -q advice comes once a
        # pass.  The first two calls are read together and pause all
        # re-reads (5, then 15 min); from the third on the cells are read
        # alone, on the back-off of the cells read alone (compat-r3-1).  A
        # failed read alone leaves the common streak as it is, and no call
        # here works (which would reset it, load-r4-4): it stays at 2.
        cloud = LimitCloud(self.clock, {ID_B: 'HT.w'})
        d = self.daemon(cloud, args=make_args(qlimit=60))
        t0 = self.clock.t
        a, b = t0 - 4 * H, t0 - 2 * H
        cloud.add(ID_B, every(t0 - 6 * H, a) + every(a + M, b - M) + every(b, t0))
        d._register_hole(ID_B, a, b, self.sensors, now=t0)
        hole = d._recheck_holes[0]
        hole['cell'] = 30
        hole['counts'] = [0] * spd._hole_ncells(hole)      # 8 cells
        self.clock.advance(H)
        per_cycle = []
        for _ in range(48):                                 # 4 h of 5-min cycles
            before = len(cloud.sample_calls())
            d._recheck_gaps(self.sensors, 'SensorPush')
            per_cycle.append(len(cloud.sample_calls()) - before)
            self.clock.advance(5 * M)
        # Calls 0, 5 and 20 min in, then 5, 15, 35, 60 and 60 min apart.
        self.assertEqual([k * 5 for k, n in enumerate(per_cycle) if n],
                         [0, 5, 20, 25, 40, 75, 135, 195])
        self.assertEqual(max(per_cycle), 1)
        self.assertEqual((hole['done'], hole['pos'], hole['cell']), (0, 6, 30))
        self.assertEqual(hole['counts'], [0] * 8)
        self.assertEqual((d._recheck_fail_streak, d._recheck_alone_streak), (2, 6))
        advice = self.logged(logging.WARNING, 'even in 30-min cells')
        self.assertEqual(len(advice), 1)
        self.assertIn('-q/--qlimit', advice[0])
        self.assertEqual(len(self.logged(logging.WARNING, 'skipped for this pass')), 6)

    def test_one_failed_call_keeps_the_batch_together(self):
        # 18 sensors back from a whole-collector outage share one call.  It
        # fails once - a 500, a 429, a reset.  The retry serves them all
        # again: one more call, not one per sensor for the rest of the pass
        # (load-r1).  Only a cell that failed twice in a row is read alone.
        kinds = ['HT1'] * 12 + ['HTP.xw'] * 2 + ['HT.w'] * 4
        sensor_types = {f'{200000 + k}.1234567890123456': kind
                        for k, kind in enumerate(kinds)}
        for failures, sizes in ((1, [18, 18]), (2, [18, 18, 1])):
            with self.subTest(failures=failures):
                self.reset_state()
                cloud = Cloud(self.clock, sensor_types)
                d = self.daemon(cloud)
                t0 = self.clock.t
                for k, sid in enumerate(sensor_types):
                    d._register_hole(sid, t0 - 3 * H + k * 10, t0 - 5 * M + k * 10,
                                     cloud.sensors(), now=t0)
                self.clock.advance(H)
                cloud.fail = failures
                for pause in (0, 5 * M, 15 * M)[:failures + 1]:
                    self.clock.advance(pause)
                    d._recheck_gaps(cloud.sensors(), 'SensorPush')
                self.assertEqual([len(c[4]) for c in cloud.sample_calls()][:len(sizes)],
                                 sizes)
                if failures == 1:
                    self.assertEqual([h['done'] for h in d._recheck_holes], [1] * 18)

    def test_after_a_skipped_cell_the_next_one_goes_after_the_healthy_holes(self):
        # A long hole whose records a backend always refuses (older than its
        # bucket's retention) and a short healthy one, due later.  Once the
        # first cell is skipped, the next one is likely to fail as well: it
        # starts one failed call short of being skipped, so the healthy
        # hole goes first and is read at once, and the next cell is read
        # alone, after it, and skipped after one more failure (load-r4).
        t0 = self.clock.t
        a = t0 - 4 * D
        self.cloud.add(ID_A, every(a - H, a + 16 * H, 5 * M))
        self.d._register_hole(ID_A, a, a + 15 * H, self.sensors, now=t0)
        bad = self.d._recheck_holes[0]
        self.assertEqual(spd._hole_ncells(bad), 2)
        self.cloud.add(ID_B, every(t0 - 4 * H, t0 - 3 * H) + every(t0 - 2 * H, t0)
                       + every(t0 - 3 * H + M, t0 - 2 * H - M))
        self.d._register_hole(ID_B, t0 - 3 * H, t0 - 2 * H, self.sensors,
                              now=t0 + 55 * M)                  # due at 1 h 55 min
        good = self.d._recheck_holes[1]
        original = self.w.write

        def write(records):
            if any(r['tags']['sensor_id'] == TAG_A for r in records):
                raise OSError('422: points beyond retention policy dropped')
            original(records)
        self.w.write = write
        self.clock.advance(H)
        for _ in range(4):                       # 1 h, 1 h 5, 1 h 10, 1 h 15
            self.w._consecutive_failures = 0     # the cycle's poll landed
            self.rechecks()
            self.clock.advance(5 * M)
        self.rechecks()                          # 1 h 20: the third failure
        fail = bad['_fail']                      # a carry: it has not failed yet
        self.assertEqual((bad['pos'], fail['k'], fail['n'], fail['at']), (1, 1, 2, None))
        self.assertEqual(len(self.cloud.sample_calls()), 3)
        self.clock.advance(35 * M)               # the back-off: 1 h 55 min
        self.w._consecutive_failures = 0
        self.rechecks()
        calls = self.cloud.sample_calls()[3:]
        self.assertEqual([c[4] for c in calls], [(ID_B,), (ID_A,)])
        self.assertEqual(good['done'], 1)
        self.assertEqual((bad['done'], bad['pos']), (1, 0))

    def test_failed_calls_on_the_last_read_lose_no_late_history(self):
        # An HT1 away for 2 days; its history reaches the cloud 5 days after
        # it returned.  From the 7-d read on, re-read calls fail for 25 min
        # (the polls get through: they are retried, a re-read is not).  The
        # last read has no next one, so its cell is not skipped after 3
        # failed calls but tried again until it works (corr-r2).
        cloud = Cloud(self.clock, {ID_A: 'HT1'})
        w = fake_writer()
        d = self.daemon(cloud, writer=w)
        t0 = self.clock.t
        away, back = t0 - 2 * D, t0
        cloud.add(ID_A, every(away - H, away, 5 * M) + every(back, back + 9 * D, 5 * M))
        d._register_hole(ID_A, away, back, cloud.sensors(), now=t0)
        history = every(away + 5 * M, back - 5 * M, 5 * M)
        bad = (t0 + 7 * D, t0 + 7 * D + 25 * M)
        while self.clock.t < t0 + 8 * D:
            self.clock.advance(5 * M)
            if abs(self.clock.t - (t0 + 5 * D)) < 1:
                cloud.add(ID_A, history)
            cloud.fail = 10 ** 6 if bad[0] <= self.clock.t < bad[1] else 0
            d._recheck_gaps(cloud.sensors(), 'SensorPush')
        self.assertTrue(set(history) <= set(written_times(w)))
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(len(self.logged(logging.WARNING, 'this is its last re-read')), 1)
        self.assertEqual(self.logged(logging.WARNING, 'skipped for this pass'), [])

    def test_the_last_read_is_given_up_a_day_after_it_was_due(self):
        # A window that never works - every call fails, or the answer is
        # truncated even in 30-min cells - is tried on the last read, as the
        # back-off allows, until a day after that read was due; then it is
        # given up and the hole finished.
        for kind in ('failed', 'truncated'):
            with self.subTest(kind):
                self.reset_state()
                del self.records[:]
                cloud = LimitCloud(self.clock, {ID_B: 'HT.w'})
                d = self.daemon(cloud, args=make_args(qlimit=60 if kind == 'truncated' else 0))
                t0 = self.clock.t
                a, b = t0 - 3 * H, t0 - 2 * H
                cloud.add(ID_B, every(a - 2 * H, b + 2 * H))
                d._register_hole(ID_B, a, b, self.sensors, now=t0)
                hole = d._recheck_holes[0]
                hole['done'] = 4                      # only the 7-d read is left
                if kind == 'truncated':
                    hole['cell'] = 30
                    hole['counts'] = [0] * spd._hole_ncells(hole)
                self.clock.advance(7 * D)
                calls = 0
                while self.clock.t < t0 + 8 * D + 8 * H:
                    if self.clock.t < t0 + 8 * D:
                        calls = len(cloud.sample_calls())
                        self.assertIn(hole, d._recheck_holes)
                    cloud.fail = 10 ** 6 if kind == 'failed' else 0
                    d._recheck_gaps(self.sensors, 'SensorPush')
                    self.clock.advance(5 * M)
                # About one try an hour, once the back-off reached its cap.
                self.assertTrue(20 <= calls <= 30, calls)
                self.assertEqual((hole['done'], hole['pos']), (5, 0))
                self.assertEqual(d._recheck_holes, [])
                self.assertTrue(self.logged(logging.WARNING, 'its last re-read is given up'))
                self.assertEqual(len(self.logged(logging.INFO, 're-checks finished')), 1)

    def test_a_read_that_is_the_last_after_downtime_keeps_its_cell(self):
        # The later re-read times went by while no re-read could run (the Pi
        # was off, the polls or a backend failed), so the read due now is
        # also the last: the offsets after it collapse into it.  Its first 3
        # calls fail, or are truncated even in 30-min cells.  The cell was
        # skipped "for the next pass", which never came, and the HT1 history
        # in it was lost (corr-r2-1).
        for kind, schedule, done, read_at in (
                ('failed', '1h, 6h, 1d, 3d, 7d', 3, 7 * D + 2 * H),
                ('failed', '1h, 6h, 1d, 3d, 7d', 2, 7 * D + 2 * H),
                ('failed', '1h, 6h, 1d', 1, D + 2 * H),
                ('truncated', '1h, 6h, 1d, 3d, 7d', 3, 7 * D + 2 * H)):
            with self.subTest(kind=kind, schedule=schedule, done=done):
                self.reset_state()
                del self.records[:]
                cloud = Cloud(self.clock, {ID_A: 'HT1'})
                w = fake_writer()
                d = self.daemon(cloud, writer=w, recheck_gaps=schedule)
                t0 = self.clock.t
                a, b = t0 - 4 * H, t0 - H
                cloud.add(ID_A, every(a - H, a, 5 * M) + every(b, b + 2 * D, 5 * M))
                d._register_hole(ID_A, a, b, cloud.sensors(), now=t0)
                hole = d._recheck_holes[0]
                hole['done'] = done                  # the earlier reads found nothing
                history = every(a + 5 * M, b - 5 * M, 5 * M)
                cloud.add(ID_A, history)
                self.clock.t = t0 + read_at
                if kind == 'failed':
                    cloud.fail = 3
                else:
                    # 30-min cells; the first 3 answers for a cell that holds
                    # history are truncated.
                    hole['cell'] = 30
                    hole['counts'] = [0] * spd._hole_ncells(hole)
                    left, real = [3], cloud.get_samples

                    def get_samples(start, stop, measures=None, limit=0, sensors=None,
                                    real=real, left=left, history=history):
                        answer = real(start, stop, measures, limit, sensors)
                        c = spd._observed_epoch(start) + 30 * M
                        d_ = spd._observed_epoch(stop) - 30 * M
                        if left[0] and any(c <= t <= d_ for t in history):
                            left[0] -= 1
                            answer['truncated'] = True
                        return answer
                    cloud.get_samples = get_samples
                for _ in range(16):                  # 75 min of 5-min cycles
                    d._recheck_gaps(cloud.sensors(), 'SensorPush')
                    self.clock.advance(5 * M)
                self.assertTrue(set(history) <= set(written_times(w)))
                self.assertEqual(d._recheck_holes, [])
                self.assertEqual(len(self.logged(logging.WARNING, 'this is its last re-read')), 1)
                self.assertEqual(self.logged(logging.WARNING, 'skipped for this pass'), [])

    def test_a_cell_skipped_for_a_later_read_still_gets_the_last_one(self):
        # An HT1 hole of two cells, its history in the first.  At its 3-d
        # read the first cell fails 3 times and is skipped: the 7-d read
        # comes later.  Then re-reads cannot run for 4 days (a backend is
        # down, say).  When the 3-d pass ends, the 7-d offset has gone by
        # too - collapsed into it, the skipped cell would never be read
        # again.  The 7-d pass is kept, and it writes the history.
        cloud = Cloud(self.clock, {ID_A: 'HT1'})
        w = fake_writer()
        d = self.daemon(cloud, writer=w)
        t0 = self.clock.t
        a, b = t0 - 14 * H, t0 - H
        cloud.add(ID_A, every(a - H, a, 5 * M) + every(b, b + 9 * D, 5 * M))
        d._register_hole(ID_A, a, b, cloud.sensors(), now=t0)
        hole = d._recheck_holes[0]
        self.assertEqual(spd._hole_ncells(hole), 2)
        hole['done'] = 3                             # the earlier reads found nothing
        history = every(a + 5 * M, a + 9 * H, 5 * M)
        cloud.add(ID_A, history)
        self.clock.advance(3 * D)
        cloud.fail = 3
        for _ in range(5):                           # 3 d, 3 d 5 min, ... 3 d 20 min
            d._recheck_gaps(cloud.sensors(), 'SensorPush')
            self.clock.advance(5 * M)
        self.assertEqual((hole['done'], hole['pos']), (3, 1))
        self.assertEqual(len(self.logged(logging.WARNING, 'skipped for this pass')), 1)
        self.clock.advance(4 * D + H)                # past the 7-d offset
        d._recheck_gaps(cloud.sensors(), 'SensorPush')
        self.assertTrue(set(history) <= set(written_times(w)))
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read 5/5')), 1)

    def test_a_failing_last_read_holds_up_no_other_gap(self):
        # The last read of sensor A's gap fails on every call; it is tried on
        # for a day, and given up by the first call after that.  Sensor B,
        # an HT.w, has a new gap every 2 hours.  Each failed try of A paused
        # all re-reads - 5 min after each read of B, longer in between - so
        # B's reads waited up to 51 min.  After its 2nd failure A's cell is
        # read alone, on the back-off the cells read alone share; B's reads
        # are made in the cycle they fall due (load-r2-2).
        cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT.w'})
        real = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            if ID_A in (sensors or ()):
                cloud.calls.append(('get_samples', 'A', start, stop, tuple(sensors), limit))
                raise ValueError('API request failed with status 500')
            return real(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        sensors = cloud.sensors()
        d = self.daemon(cloud)
        t0 = self.clock.t
        d._register_hole(ID_A, t0 - 7 * D - 3 * H, t0 - 7 * D, sensors, now=t0 - 7 * D)
        d._recheck_holes[0]['done'] = 4              # only the 7-d read is left
        waits, next_b = [], t0 + 10 * M
        while self.clock.t < t0 + 2 * D:
            if self.clock.t >= next_b:
                d._register_hole(ID_B, self.clock.t - 80 * M, self.clock.t - 20 * M,
                                 sensors, now=self.clock.t)
                next_b += 2 * H
            before = [(h, h['done']) for h in d._recheck_holes
                      if h['sid'] == ID_B and h['pos'] == 0]
            d._recheck_gaps(sensors, 'SensorPush')
            for h, done in before:
                if h['done'] > done or not any(x is h for x in d._recheck_holes):
                    waits.append(self.clock.t - (h['closed_at'] + OFFSETS[done]))
            self.clock.advance(5 * M)
        tries = [c for c in cloud.calls if c[1] == 'A']
        self.assertTrue(20 <= len(tries) <= 30, len(tries))
        self.assertEqual([h['sid'] for h in d._recheck_holes].count(ID_A), 0)
        self.assertGreater(len(waits), 20)
        self.assertLess(max(waits), 5 * M, max(waits) / 60)

    def test_one_failed_call_early_in_a_long_outage_costs_one_more_call(self):
        # A whole-house outage of 23 days: the HT1 holes are clamped to the
        # 21 days on board, so the first 4 cells of the pass are the HT.w
        # holes' alone.  The first call fails once.  Its cell was put after
        # every healthy one: the HT1 holes went first, alone, through all
        # their cells, and the call that serves all four sensors never formed
        # again - 47 calls more.  Now the cell is retried in its turn, with
        # the others of its call: one call more (load-r2-1).
        kinds = {f'{300000 + k}.1234567890123456': kind
                 for k, kind in enumerate(['HT.w', 'HT.w', 'HT1', 'HT1'])}
        counts = []
        for failures in (0, 1):
            self.reset_state()
            cloud = Cloud(self.clock, kinds)
            d = self.daemon(cloud)
            t0 = self.clock.t
            for sid in kinds:
                d._register_hole(sid, t0 - 23 * D, t0 - 5 * M, cloud.sensors(), now=t0)
            self.clock.advance(H)
            cloud.fail = failures
            for _ in range(100):
                if all(h['done'] for h in d._recheck_holes):
                    break
                d._recheck_gaps(cloud.sensors(), 'SensorPush')
                self.clock.advance(5 * M)
            calls = cloud.sample_calls()
            counts.append(len(calls))
            sizes = [len(c[4]) for c in calls]
            self.assertEqual((sizes.count(2), sizes.count(4)), (4 + failures, 47))
            if failures:
                self.assertEqual(calls[1][2:5], calls[0][2:5])   # the same call again
        self.assertEqual(counts, [51, 52])

    def test_a_probe_with_an_empty_interior_writes_its_margins(self):
        # After a pause of 45-60 min, an HT1's own sample just before the
        # newest copy seen can sit in the chunk the window left out, while
        # the probe's interior is empty.  The probe's one read writes its
        # cell anyway, margins included (corr-r3).
        t0 = self.clock.t
        own = t0 - 3 * H - 40                    # never in a fetched window
        a, b = t0 - 3 * H - 15, t0 - 2 * H - 53 * M
        self.cloud.add(ID_A, [own, a, b])
        self.d._register_hole(ID_A, a, b, self.sensors, now=t0, probe=90 * M)
        self.clock.advance(H)
        self.rechecks()
        self.assertEqual(written_times(self.w), [own, a, b])
        self.assertEqual(self.d._recheck_holes, [])
        # The log says so, at DEBUG: nothing new inside the stretch, but 3
        # samples around it written.  It said 'nothing new' (corr-r2-2).
        self.assertEqual(self.logged(logging.INFO, 'Read again'), [])
        summary = self.logged(logging.DEBUG, 'Read again what the polls may have missed')
        self.assertEqual(len(summary), 1)
        self.assertTrue(summary[0].endswith('): 0 new samples, 3 written'), summary[0])
        self.assertEqual(self.logged(logging.DEBUG, 'nothing new'), [])
        self.assertEqual(len(self.logged(logging.DEBUG, '0 samples inside')), 1)

    def test_a_failing_hole_is_left_out_and_dropped_after_three_errors(self):
        t0 = self.clock.t
        a, b = t0 - 3 * H, t0 - 2 * H
        for sid in (ID_A, ID_C):
            self.cloud.add(sid, every(a - H, a) + every(b, t0)
                           + every(a + 5 * M, b - 5 * M, 5 * M))
            self.d._register_hole(sid, a, b, self.sensors, now=t0)
        good = self.d._recheck_holes[1]
        original = spd.process_samples

        def broken(samples, *rest):
            if ID_A in samples['sensors']:
                raise KeyError('temperature')
            return original(samples, *rest)
        self.clock.advance(H)
        with mock.patch.object(spd, 'process_samples', broken):
            self.rechecks()
        # The other hole of the same call went ahead; the failing one was
        # tried twice more in the same cycle, then dropped.
        self.assertEqual(self.d._recheck_holes, [good])
        self.assertEqual(good['done'], 1)
        self.assertEqual(len(self.cloud.sample_calls()), 3)
        self.assertEqual(len(self.logged(logging.WARNING, 'Could not re-read the gap')), 2)
        errors = [r for r in self.records if r.levelno == logging.ERROR]
        self.assertEqual(len(errors), 1)
        self.assertIn('after 3 failed re-reads in a row', errors[0].getMessage())
        self.assertIsNotNone(errors[0].exc_info)
        self.assertEqual(written_times(self.w),
                         sorted(t for t in self.cloud.samples[ID_C] if a - H <= t <= b + H))

    def test_writes_in_slices_of_at_most_5000_records(self):
        t0 = self.clock.t
        a, b = t0 - 8 * H, t0 - 2 * H
        self.d._register_hole(ID_B, a, b, self.sensors, now=t0)
        dense = every(a - H, b + H, 5)          # a sample every 5 s
        self.cloud.add(ID_B, dense)
        self.clock.advance(H)
        self.rechecks()
        sizes = [len(batch) for batch in self.w.written]
        self.assertEqual(sizes, [5000, len(dense) - 5000])

    def test_budget(self):
        for delay, call_seconds, calls, sleeps in (
                (0, 0, 6, [0] * 5), (10, 1, 6, [10] * 5), (50, 1, 3, [50, 50]),
                (60, 1, 2, [60])):
            with self.subTest(delay=delay):
                self.reset_state()
                cloud = Cloud(self.clock, {ID_A: 'HT1'})
                cloud.call_seconds = call_seconds
                d = self.daemon(cloud, args=make_args(delay=delay))
                t0 = self.clock.t
                for k in range(10):         # two days apart: one call each
                    start = t0 - 19 * D + k * 2 * D
                    d._register_hole(ID_A, start, start + H, cloud.sensors(), now=t0)
                self.clock.t = t0 + H
                del self.sleeps[:]
                d._recheck_gaps(cloud.sensors(), 'SensorPush')
                self.assertEqual(len(cloud.sample_calls()), calls)
                # No sleep after the last call.
                self.assertEqual(self.sleeps, sleeps)
                self.assertEqual(sum(h['done'] for h in d._recheck_holes), calls)

    def test_one_call_for_every_sensor_after_a_whole_collector_outage(self):
        kinds = ['HT1'] * 12 + ['HTP.xw'] * 2 + ['HT.w'] * 4
        sensor_types = {f'{200000 + k}.1234567890123456': kind
                        for k, kind in enumerate(kinds)}
        cloud = Cloud(self.clock, sensor_types)
        t0 = self.clock.t
        down, up = t0 - 3 * H, t0 - 5 * M        # gateways, internet, daemon
        for k, (sid, kind) in enumerate(sensor_types.items()):
            step = 7 * M if kind == 'HT1' else M
            # An HT1's last record before can be well before the outage.
            last = down - (k * 3 * M if kind == 'HT1' else 0)
            cloud.add(sid, every(last - 2 * H, last, step) + [up + k * 10])
        d = self.daemon(cloud)
        d._recheck_last_seen = {sid: max(t for t in times if t <= down)
                                for sid, times in cloud.samples.items()}
        self.fetch(d, cloud, down - H)           # the catch-up after the restart
        self.assertEqual(len(d._recheck_holes), 18)
        self.clock.advance(H)
        d._recheck_gaps(cloud.sensors(), 'SensorPush')
        calls = cloud.sample_calls()
        self.assertEqual(len(calls), 1)
        self.assertEqual(set(calls[0][4]), set(sensor_types))
        self.assertEqual([h['done'] for h in d._recheck_holes], [1] * 18)

    def test_cells_older_than_max_backfill_are_skipped(self):
        t0 = self.clock.t
        d = self.daemon(self.cloud, writer=self.w, max_backfill='2d')
        d._register_hole(ID_B, t0 - 4 * D, t0 - H, self.sensors, now=t0)
        hole = d._recheck_holes[0]
        # Some of it arrived (else the long-hole skip would apply, below).
        self.cloud.add(ID_B, every(t0 - D, t0 - 2 * H, 10 * M))
        self.clock.advance(H)
        cap = self.clock.t - 2 * D
        wanted = [k for k in range(spd._hole_ncells(hole))
                  if spd._hole_cell(hole, k)[0] >= cap]
        self.assertTrue(0 < len(wanted) < spd._hole_ncells(hole))
        self.rechecks(d)
        self.rechecks(d)                       # a pass longer than one cycle
        calls = self.cloud.sample_calls()
        self.assertEqual(len(calls), len(wanted))
        self.assertTrue(all(spd._observed_epoch(c[2]) >= cap - 30 * M for c in calls))
        self.assertEqual((hole['done'], hole['pos']), (1, 0))
        # A hole wholly older than that finishes its pass without a call.
        d._register_hole(ID_A, t0 - 10 * D, t0 - 9 * D, self.sensors, now=self.clock.t)
        self.clock.advance(H)
        self.rechecks(d)
        self.assertEqual(len(self.cloud.sample_calls()), len(wanted))
        self.assertEqual(d._recheck_holes[-1]['done'], 1)
        self.assertEqual(len(self.logged(logging.INFO, 'older than MAX_BACKFILL')), 1)

    def test_early_completion_for_one_per_minute_sensors_only(self):
        t0 = self.clock.t
        a, b = t0 - 3 * H, t0 - 2 * H
        for sid in (ID_A, ID_B, ID_C):          # HT1, HT.w, HTP.xw
            self.cloud.add(sid, every(a - H, a) + every(b, t0))
            self.d._register_hole(sid, a, b, self.sensors, now=t0)
            arrived = every(a + M, b - M)
            if sid == ID_B:                     # three minutes still missing
                arrived = [t for t in arrived if not a + 20 * M <= t <= a + 22 * M]
            self.cloud.add(sid, arrived)
        self.clock.advance(H)
        self.rechecks()
        self.assertEqual([(h['sid'], h['done']) for h in self.d._recheck_holes],
                         [(ID_A, 1), (ID_B, 1)])
        self.assertEqual(len(self.logged(logging.INFO, 'is complete (59 samples)')), 1)

    def test_a_window_that_always_fails_holds_up_no_other_hole(self):
        # The backend always refuses one hole's records (say, older than a
        # bucket's retention).  The other hole is read anyway, and the bad
        # cell costs RECHECK_CELL_MAX_FAILS calls a pass, then waits for the
        # next pass.
        t0 = self.clock.t
        for sid, a in ((ID_A, t0 - 3 * D), (ID_B, t0 - 3 * H)):
            self.cloud.add(sid, every(a - H, a + 2 * H, 5 * M))
            self.d._register_hole(sid, a, a + H, self.sensors, now=t0)
        bad, good = self.d._recheck_holes
        original = self.w.write

        def write(records):
            if any(r['tags']['sensor_id'] == TAG_A for r in records):
                raise OSError('422: points beyond retention policy dropped')
            original(records)
        self.w.write = write
        self.clock.advance(H)
        for _ in range(12):                      # an hour of 5-min cycles
            self.w._consecutive_failures = 0     # the cycle's poll landed
            self.rechecks()
            self.clock.advance(5 * M)
        self.assertEqual(good['done'], 1)
        self.assertEqual((bad['done'], bad['pos'], bad['counts']), (1, 0, [0]))
        self.assertEqual(len([c for c in self.cloud.sample_calls() if ID_A in c[4]]), 3)
        self.assertEqual(len(self.logged(logging.WARNING, 'failed 3 times in a row')), 1)
        # The next pass reads it again.
        self.clock.t = t0 + 6 * H
        self.w._consecutive_failures = 0
        self.w.write = original
        self.rechecks()
        self.assertEqual((bad['done'], bad['counts']), (2, [11]))

    def test_a_long_gap_found_complete_is_not_read_again(self):
        # Sensors that record every minute, away for 30 h: three cells.
        # One backlog is complete by the first re-read - read once - the
        # other lacks 3 minutes and is read on.
        t0 = self.clock.t
        a, b = t0 - 31 * H, t0 - H
        for sid in (ID_B, ID_C):
            self.cloud.add(sid, every(a - 2 * H, a) + every(b, t0))
            self.d._register_hole(sid, a, b, self.sensors, now=t0)
            arrived = every(a + M, b - M)
            if sid == ID_C:
                arrived = [t for t in arrived
                           if not t0 - 20 * H <= t <= t0 - 20 * H + 2 * M]
            self.cloud.add(sid, arrived)
        self.assertEqual(spd._hole_ncells(self.d._recheck_holes[0]), 3)
        self.clock.advance(H)
        self.rechecks()
        self.assertEqual(len(self.cloud.sample_calls()), 3)
        self.assertEqual([(h['sid'], h['done']) for h in self.d._recheck_holes],
                         [(ID_C, 1)])
        self.assertEqual(len(self.logged(logging.INFO, 'is complete (1799 samples)')), 1)

    def test_a_pass_not_read_in_one_go_proves_nothing(self):
        # A restart in the middle of a pass loses what the earlier cells
        # showed: the hole is not dropped on that pass (it is read again).
        t0 = self.clock.t
        a, b = t0 - 31 * H, t0 - H
        self.cloud.add(ID_B, every(a - 2 * H, b + H))
        self.d._register_hole(ID_B, a, b, self.sensors, now=t0)
        self.clock.advance(H)
        with mock.patch.object(spd, 'RECHECK_CALLS_PER_CYCLE', 1):
            self.rechecks()
            self.d._save_state()
            d = self.daemon(self.cloud, writer=self.w)
            self.assertEqual(d._recheck_holes[0]['pos'], 1)
            self.rechecks(d)
            self.rechecks(d)
        self.assertEqual([(h['done'], h['pos']) for h in d._recheck_holes], [(1, 0)])

    def test_long_hole_with_nothing_found_skips_to_the_3d_read(self):
        t0 = self.clock.t
        a, b = t0 - 2 * D - 2 * H, t0 - 2 * H      # an HT1 away for two days
        self.cloud.add(ID_A, every(a - H, a, 5 * M) + every(b, t0, 5 * M))
        self.d._register_hole(ID_A, a, b, self.sensors, now=t0)
        hole = self.d._recheck_holes[0]
        self.d._register_hole(ID_B, t0 - 3 * H, t0 - 2 * H, self.sensors, now=t0)
        short = self.d._recheck_holes[1]
        self.clock.t = t0 + H
        self.rechecks()                          # 5 cells + 1: one pass each
        self.assertEqual((hole['done'], hole['pos']), (3, 0))
        self.assertEqual(short['done'], 1)      # short holes keep every offset
        self.assertEqual(len(self.logged(logging.INFO, 'next re-read after 3d')), 1)
        # One INFO line per pass, the cells only at DEBUG.
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read ')), 2)
        self.assertEqual(len(self.logged(logging.DEBUG, 'Re-read cell')), 6)
        calls = len(self.cloud.sample_calls())
        self.clock.t = t0 + 3 * D
        self.rechecks()
        self.rechecks()
        self.clock.t = t0 + 7 * D
        self.rechecks()
        self.rechecks()
        self.assertNotIn(hole, self.d._recheck_holes)
        # 3 passes of 5 cells instead of 5.
        long_calls = [c for c in self.cloud.sample_calls() if ID_A in c[4]]
        self.assertEqual(len(long_calls), 15)
        self.assertGreater(len(self.cloud.sample_calls()), calls)


# -----------------------------------------------------------------------------
# Failure handling: the last pass, cells read alone
# -----------------------------------------------------------------------------
class LastPass(RecheckCase):
    """A pass under way when re-reads stop, resumed after the last offset.

    Round 3 let the later offsets collapse into it, so the cells it had read
    or skipped before the stop got no read after it: HT1 history that reached
    the cloud in between was never written (corr-r3-1).  Now a pass is the
    last only if it is the last offset's or began after the last offset.
    """

    def setUp(self):
        super().setUp()
        self.cloud = Cloud(self.clock, {ID_A: 'HT1'})
        self.sensors = self.cloud.sensors()

    def cycles(self, d, until, step=5 * M):
        while self.clock.t < until:
            d._recheck_gaps(self.sensors, 'SensorPush')
            self.clock.advance(step)

    def two_day_gap(self, d, first_read=3):
        """An HT1 away 2 days (5 cells).  The first read finds nothing: the
        next is the 3d read, or the 6h read without a 3d one (first_read=1).
        Returns the gap's history, which each test adds to the cloud late."""
        t0 = self.clock.t
        a, b = t0 - 2 * D - H, t0 - H
        self.cloud.add(ID_A, every(a - 2 * H, a, 5 * M) + every(b, b + 9 * D, 5 * M))
        d._register_hole(ID_A, a, b, self.sensors, now=t0)
        hole = d._recheck_holes[0]
        self.assertEqual(spd._hole_ncells(hole), 5)
        self.clock.t = t0 + H
        self.cycles(d, t0 + H + 30 * M)
        self.assertEqual((hole['done'], hole['pos']), (first_read, 0))
        return t0, hole, every(a + 5 * M, b - 5 * M, 5 * M)

    def assert_history_written(self, d, w, history):
        self.assertTrue(set(history) <= set(written_times(w)))
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(len(self.logged(logging.INFO, 're-checks finished; the cloud '
                                         'holds %d samples' % len(history))), 1)

    def test_a_backend_failing_from_inside_the_3d_read_until_after_7d(self):
        # --delay 60: 3 calls a cycle, so the 3d pass reads cells 0-2 at 3d.
        # Then a second backend fails its writes until 7d + 2 h; re-reads
        # wait meanwhile.  Round 3 lost 383 of 575 samples.
        wa, wb = fake_writer(), fake_writer()
        d = self.make_daemon([wa, wb], api=self.cloud, args=make_args(delay=60))
        d._interruptible_sleep = self.sleep
        t0, hole, history = self.two_day_gap(d)
        self.clock.t = t0 + 3 * D
        d._recheck_gaps(self.sensors, 'SensorPush')
        self.assertEqual(hole['pos'], 3)
        wb._consecutive_failures = 1
        self.cycles(d, t0 + 3 * D + 12 * H)
        self.cloud.add(ID_A, history)
        self.cycles(d, t0 + 3 * D + 13 * H)
        self.clock.t = t0 + 7 * D + 2 * H
        wb._consecutive_failures = 0
        self.cycles(d, t0 + 7 * D + 4 * H)
        self.assert_history_written(d, wa, history)
        # The 3d pass ended late, and the 7d pass came after it.
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read 4/5')), 1)
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read 5/5')), 1)

    def test_the_pi_off_from_inside_the_3d_read_until_after_7d(self):
        # The same stop, but the daemon is down: the hole comes back from
        # state.json in the middle of its 3d pass.  Round 3 lost 383 of 575.
        w = fake_writer()
        d = self.daemon(self.cloud, writer=w, args=make_args(delay=60))
        t0, hole, history = self.two_day_gap(d)
        self.clock.t = t0 + 3 * D
        d._recheck_gaps(self.sensors, 'SensorPush')
        d._save_state()
        self.cloud.add(ID_A, history)
        self.clock.t = t0 + 7 * D + 2 * H
        d = self.daemon(self.cloud, writer=w, args=make_args(delay=60))
        restored = d._recheck_holes[0]
        pos, since = restored['pos'], restored.get('_since')
        self.cycles(d, t0 + 7 * D + 4 * H)
        self.assert_history_written(d, w, history)
        # When the pass began is not saved: it counts from its due time.
        self.assertEqual((pos, since), (3, t0 + 3 * D))

    def test_a_backend_failing_from_inside_the_6h_read_until_after_1d(self):
        # RECHECK_GAPS = 1h, 6h, 1d (the README's schedule without a 3d
        # read): the 6h pass has read 2 cells when a second backend fails,
        # until 1d + 2 h; the history comes at 12 h.  Round 3 lost 251.
        self.cloud.call_seconds = 1
        wa, wb = fake_writer(), fake_writer()
        d = self.make_daemon([wa, wb], api=self.cloud, args=make_args(delay=60),
                             recheck_gaps='1h, 6h, 1d')
        d._interruptible_sleep = self.sleep
        t0, hole, history = self.two_day_gap(d, first_read=1)
        self.clock.t = t0 + 6 * H
        d._recheck_gaps(self.sensors, 'SensorPush')
        self.assertEqual(hole['pos'], 2)
        wb._consecutive_failures = 1
        self.cycles(d, t0 + 12 * H)
        self.cloud.add(ID_A, history)
        self.cycles(d, t0 + 13 * H)
        self.clock.t = t0 + D + 2 * H
        wb._consecutive_failures = 0
        self.cycles(d, t0 + D + 4 * H)
        self.assert_history_written(d, wa, history)

    def test_a_cell_skipped_for_a_later_read_and_a_restart(self):
        # test_a_cell_skipped_for_a_later_read_still_gets_the_last_one, with
        # a restart right after the skip: round 3 kept that last pass with
        # a flag in memory, which the restart lost (108 of 108 lost).
        w = fake_writer()
        d = self.daemon(self.cloud, writer=w)
        t0 = self.clock.t
        a, b = t0 - 14 * H, t0 - H
        self.cloud.add(ID_A, every(a - H, a, 5 * M) + every(b, b + 9 * D, 5 * M))
        d._register_hole(ID_A, a, b, self.sensors, now=t0)
        hole = d._recheck_holes[0]
        hole['done'] = 3                             # the earlier reads found nothing
        history = every(a + 5 * M, a + 9 * H, 5 * M)
        self.cloud.add(ID_A, history)
        self.clock.advance(3 * D)
        self.cloud.fail = 3
        self.cycles(d, self.clock.t + 25 * M)
        self.assertEqual((hole['done'], hole['pos']), (3, 1))
        self.assertEqual(len(self.logged(logging.WARNING, 'skipped for this pass')), 1)
        d._save_state()
        self.clock.advance(4 * D + H)                # past the 7-d offset
        d = self.daemon(self.cloud, writer=w)
        self.cycles(d, self.clock.t + 15 * M)
        self.assert_history_written(d, w, history)
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read 5/5')), 1)

    def test_a_pass_that_begins_after_the_last_offset_is_the_last(self):
        # The other side (corr-r2-1): re-reads could not run until after the
        # 7d time, so the pass due then is the last one.  It reads each cell
        # once, and no 7d pass follows.
        w = fake_writer()
        d = self.daemon(self.cloud, writer=w)
        t0, hole, history = self.two_day_gap(d)
        self.cloud.add(ID_A, history)
        calls = len(self.cloud.sample_calls())
        self.clock.t = t0 + 7 * D + 2 * H
        self.cycles(d, t0 + 7 * D + 3 * H)
        self.assertEqual(len(self.cloud.sample_calls()) - calls, 5)
        self.assert_history_written(d, w, history)
        self.assertEqual(len(self.logged(logging.INFO, 'Re-read 5/5')), 1)
        self.assertEqual(self.logged(logging.INFO, 'Re-read 4/5'), [])


class ReadAlone(RecheckCase):
    """Cells read alone: one back-off of their own, their order, regroups."""

    def kept_cells(self, n, b_every=None, c_at=None, days=2):
        """n HT1 gaps of one outage closed 7 days ago; their history came
        after the 3d read, and the backend refuses it (older than a bucket's
        7-day retention: 422), so their 7d reads fail for good.  B (HT.w)
        gets a new gap every ``b_every`` seconds, C (HT.w) one at ``c_at``
        whose first two calls fail (a 502).  5-min cycles, as many days.
        Returns the calls, B's waits and C's times."""
        a_ids = ['%d.4567890123456789' % (300100 + k) for k in range(n)]
        kinds = dict.fromkeys(a_ids, 'HT1')
        kinds.update({ID_B: 'HT.w', ID_C: 'HT.w'})
        cloud = Cloud(self.clock, kinds)
        sensors = cloud.sensors()
        w = fake_writer()
        a_tags = {spd.sensor_id_tag(s) for s in a_ids}
        real_write = w.write

        def write(records):
            if any(r['tags']['sensor_id'] in a_tags for r in records):
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        c_left = [2]
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            if ID_C in (sensors or ()) and c_left[0]:
                c_left[0] -= 1
                cloud.calls.append(('get_samples', 'failed', start, stop,
                                    tuple(sensors), limit))
                raise ValueError('API request failed with status 502')
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        d = self.daemon(cloud, writer=w)
        t0 = self.clock.t
        a_end = t0 - 7 * D + 60
        for sid in a_ids:
            cloud.add(sid, every(a_end - 6 * H, a_end + 3 * H, 2 * M))
            d._register_hole(sid, a_end - 3 * H, a_end, sensors, now=a_end)
            d._recheck_holes[-1]['done'] = 4         # only the 7d read is left
        cloud.add(ID_B, every(t0 - 2 * H, t0 + (days + 1) * D))
        cloud.add(ID_C, every(t0 - 2 * H, t0 + (days + 1) * D))
        out = {'a_ids': a_ids, 'grace_end': a_end + 7 * D + spd.RECHECK_LAST_PASS_GRACE,
               'calls': [], 'b_waits': [], 'c_twice': None, 'c_read': None}
        next_b, c_hole = t0 + 10 * M, None
        while self.clock.t < t0 + days * D:
            w._consecutive_failures = 0              # the cycle's poll landed
            if b_every and self.clock.t >= next_b:
                d._register_hole(ID_B, self.clock.t - 80 * M, self.clock.t - 20 * M,
                                 sensors, now=self.clock.t)
                next_b += b_every
            if c_at is not None and c_hole is None and self.clock.t >= t0 + c_at:
                d._register_hole(ID_C, self.clock.t - 80 * M, self.clock.t - 20 * M,
                                 sensors, now=self.clock.t)
                c_hole = d._recheck_holes[-1]
            before = [(h, h['done']) for h in d._recheck_holes
                      if h['sid'] == ID_B and h['pos'] == 0]
            n0 = len(cloud.calls)
            d._recheck_gaps(sensors, 'SensorPush')
            for c in cloud.calls[n0:]:
                if c[0] == 'get_samples':
                    out['calls'].append((self.clock.t, c[1], c[4]))
            for h, done in before:
                if h['done'] > done or not any(x is h for x in d._recheck_holes):
                    out['b_waits'].append(self.clock.t - (h['closed_at'] + OFFSETS[done]))
            if c_hole is not None and out['c_twice'] is None and not c_left[0]:
                # C's window failed twice: when the common back-off ends.
                out['c_twice'] = self.clock.t + d._recheck_next_at - self.clock.mono
            if (c_hole is not None and out['c_read'] is None
                    and not any(x is c_hole for x in d._recheck_holes)):
                out['c_read'] = self.clock.t
            self.clock.advance(5 * M)
        out['d'] = d
        return out

    def test_kept_last_reads_share_one_back_off_and_hold_up_no_other_gap(self):
        # 12 kept cells: round 3 gave each one a back-off of its own, so
        # together they made a failing call in nearly every cycle (272 in two
        # days, 816 ERROR lines; compat-r3-1, load-r3-3), and their give-ups
        # paused every other gap (load-r2-2).
        run = self.kept_cells(12, b_every=2 * H)
        a_ids, d = run['a_ids'], run['d']
        a_calls = [(t, s) for t, _, s in run['calls'] if set(s) & set(a_ids)]
        # The first two calls serve all 12; one call a step after that.
        self.assertEqual([len(s) for _, s in a_calls[:2]], [12, 12])
        in_grace = [s for t, s in a_calls if t <= run['grace_end']]
        self.assertTrue(len(in_grace) <= 32, len(in_grace))
        self.assertEqual(set(in_grace[2:]), {(sid,) for sid in a_ids})
        # After the grace end, one more call each, alone, which fails and
        # gives the cell up.  (Round 4 gave them up at the grace end without
        # a call; corr-r4-1.)
        self.assertEqual(sorted(s for t, s in a_calls if t > run['grace_end']),
                         sorted((sid,) for sid in a_ids))
        self.assertEqual(len(self.logged(logging.WARNING, 'its last re-read is given up')), 12)
        self.assertFalse([h for h in d._recheck_holes if h['sid'] in a_ids])
        # Only the two calls read together paused all re-reads.
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap re-reads pause')), 2)
        self.assertGreater(len(run['b_waits']), 20)
        self.assertLess(max(run['b_waits']), 5 * M)

    def test_a_twice_failed_window_goes_before_the_kept_cells(self):
        # C's gap is noted 2 h in, while the 12 cells are tried one by one;
        # its first two calls fail.  Round 3 put C's window after the kept
        # cells, which failed a call in every cycle: C waited 33 h
        # (load-r3-1).  Now it is read within one step of the back-off of
        # the cells read alone, before any of them.
        run = self.kept_cells(12, c_at=2 * H, days=1)
        self.assertIsNotNone(run['c_read'])
        self.assertLessEqual(run['c_read'] - run['c_twice'],
                             spd.RECHECK_BACKOFF_MAX + 5 * M)
        # No call between the end of the pause after C's second failure and
        # C's read alone, which worked.
        calls = run['calls']
        read = next(i for i, (_, tag, s) in enumerate(calls)
                    if s == (ID_C,) and tag is None)
        self.assertEqual([s for t, _, s in calls[:read] if t >= run['c_twice']], [])

    def test_a_long_last_read_given_up_after_its_grace_holds_up_no_other_gap(self):
        # A 10-day HT1 gap (22 cells) whose 7d read fails in every call, and
        # B (HT.w) with a new gap every 2 h.  After the grace day each cell
        # is given up after one call read alone, the kept one too.  Round 3
        # put those calls on the common back-off: B waited up to 56 min, for
        # 10 h (load-r2-2).
        cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT.w'})
        sensors = cloud.sensors()
        real_get = cloud.get_samples
        a_calls = []                                 # when the calls for A were made

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            if ID_A in (sensors or ()):
                a_calls.append(self.clock.t)
                raise ValueError('API request failed with status 500')
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        d = self.daemon(cloud)
        t0 = self.clock.t
        a_end = t0 - 7 * D + 60
        d._register_hole(ID_A, a_end - 10 * D, a_end, sensors, now=a_end)
        hole = d._recheck_holes[0]
        hole['done'] = 4                             # only the 7d read is left
        self.assertEqual(spd._hole_ncells(hole), 22)
        cloud.add(ID_B, every(t0 - 2 * H, t0 + 5 * D))
        grace_end = a_end + 7 * D + spd.RECHECK_LAST_PASS_GRACE
        waits, next_b, gone = [], t0 + 10 * M, None
        while self.clock.t < t0 + 3 * D:
            if self.clock.t >= next_b:
                d._register_hole(ID_B, self.clock.t - 80 * M, self.clock.t - 20 * M,
                                 sensors, now=self.clock.t)
                next_b += 2 * H
            before = [(h, h['done']) for h in d._recheck_holes
                      if h['sid'] == ID_B and h['pos'] == 0]
            d._recheck_gaps(sensors, 'SensorPush')
            for h, done in before:
                if h['done'] > done or not any(x is h for x in d._recheck_holes):
                    waits.append(self.clock.t - (h['closed_at'] + OFFSETS[done]))
            if gone is None and hole not in d._recheck_holes:
                gone = self.clock.t
            self.clock.advance(5 * M)
        self.assertIsNotNone(gone)
        self.assertLess(gone - grace_end, 23 * H)
        # In the grace day, one call a step of the back-off; after it, one,
        # alone, for each of the 22 cells.  (Round 4 gave the kept first
        # cell up at the grace end without a call: 21; corr-r4-1.)
        self.assertTrue(20 <= len([t for t in a_calls if t <= grace_end]) <= 30)
        self.assertEqual(len([t for t in a_calls if t > grace_end]), 22)
        self.assertEqual(len(self.logged(logging.WARNING, 'its last re-read is given up')), 22)
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap re-reads pause')), 2)
        self.assertGreater(len(waits), 30)
        self.assertLess(max(waits), 5 * M)

    def outage(self, days, fail_calls, kinds=('HT.w', 'HT.w', 'HT1', 'HT1'),
               refuse=None):
        """A whole-house outage of ``days``: its 1h pass, as in
        test_one_failed_call_early_in_a_long_outage_costs_one_more_call.
        The calls numbered in ``fail_calls`` (from 0) fail; the backend
        refuses the records of sensor number ``refuse``.  Returns the
        calls of the pass, and leaves only its log lines in self.records:
        a hole read alone after the others can end its 1h pass after the
        6h read of the others began (they go one gap at a time), and the
        calls of that later pass, and what they log, are left out."""
        sids = ['%d.1234567890123456' % (300000 + k) for k in range(len(kinds))]
        cloud = Cloud(self.clock, dict(zip(sids, kinds)))
        w = fake_writer()
        if refuse is not None:
            bad = spd.sensor_id_tag(sids[refuse])
            real_write = w.write

            def write(records):
                if any(r['tags']['sensor_id'] == bad for r in records):
                    raise OSError('422: points beyond retention policy dropped')
                real_write(records)
            w.write = write
        seen, real_get = [0], cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            seen[0] += 1
            if seen[0] - 1 in fail_calls:
                cloud.calls.append(('get_samples', 'failed', start, stop,
                                    tuple(sensors or ()), limit))
                raise ValueError('API request failed with status 500')
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        d = self.daemon(cloud, writer=w)
        real_call, marks = d._recheck_call, []

        def recheck_call(batch, *a, **k):
            # Whether the call is for a later pass, and where its lines begin.
            marks.append((any(e['hole']['done'] for e in batch), len(self.records)))
            return real_call(batch, *a, **k)
        d._recheck_call = recheck_call
        t0 = self.clock.t
        for sid in sids:
            cloud.add(sid, every(t0 - days * D - H, t0 - days * D, 10 * M)
                      + every(t0 - 5 * M, t0 + D, 10 * M)
                      + every(t0 - days * D + 10 * M, t0 - 15 * M, 10 * M))
            d._register_hole(sid, t0 - days * D, t0 - 5 * M, cloud.sensors(), now=t0)
        self.clock.advance(H)
        for _ in range(200):
            if all(h['done'] for h in d._recheck_holes):
                break
            w._consecutive_failures = 0               # the cycle's poll landed
            d._recheck_gaps(cloud.sensors(), 'SensorPush')
            self.clock.advance(5 * M)
        self.assertTrue(all(h['done'] for h in d._recheck_holes))
        self.assertEqual(len(marks), len(cloud.calls))
        ends = [at for _, at in marks[1:]] + [len(self.records)]
        later = {i for (is_later, start), end in zip(marks, ends) if is_later
                 for i in range(start, end)}
        self.records[:] = [r for i, r in enumerate(self.records) if i not in later]
        return [c[4] for c, (is_later, _) in zip(cloud.calls, marks) if not is_later]

    def test_two_failed_calls_early_in_a_long_outage_cost_two_more_calls(self):
        # Two failed calls in a row on one batch: round 3 then read every
        # hole alone for the rest of the pass - 151 calls instead of 51 for
        # this 23-day outage, 30 instead of 7 for a 3-day one (load-r3-2).
        # Now the first cell read alone works, so the others are read
        # together again at that window, and the batch moves on in step.
        for days, calls in ((23, 54), (3, 10)):
            with self.subTest(days=days):
                self.reset_state()
                sizes = [len(s) for s in self.outage(days, {0, 1})]
                self.assertEqual(len(sizes), calls)
                # The failed batch twice, its first cell alone, the rest
                # together at the same window; then the windows as usual.
                self.assertEqual(sizes[:4], [2, 2, 1, 1] if days == 23 else [4, 4, 1, 3])
                self.assertEqual(sizes[4:].count(4), 47 if days == 23 else 6)

    def test_a_regroup_is_tried_once_a_window(self):
        # One sensor's records are refused (sensor 2, an HT1), so every call
        # with it fails: twice together, then the regroup after sensor 0's
        # read alone worked.  Its cells are then read alone; no further
        # regroup pauses all re-reads.  Calls with sensor 2 never pause them
        # again either: each fails once, alone, and is skipped.  Counted in
        # the 1h pass only (see outage): sensor 3's cell of the failed
        # window waits behind sensor 2's carries - the gaps that keep
        # failing go one at a time (crawl-r5) - so the 6h read of the other
        # holes begins before the 1h pass ends, and its own failed calls
        # with sensor 2 pause re-reads three more times.
        calls = self.outage(3, set(), refuse=2)
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap re-reads pause')), 3)
        bad = [s for s in calls if '300002.1234567890123456' in s]
        self.assertEqual([len(s) for s in bad[:3]], [4, 4, 3])
        self.assertEqual({len(s) for s in bad[3:]}, {1})
        self.assertEqual(len(self.logged(logging.WARNING, 'skipped for this pass')), 7)

    def test_the_warnings_count_the_calls_made_for_a_cell(self):
        # A carry starts one failed call short of being skipped, and its
        # WARNING said 'failed 3 times in a row' after its one call: on a bad
        # day every cell of a long gap said so (compat-r4-2).  Each WARNING
        # now gives the failed calls made for the cell in this pass, and a
        # regroup keeps the count.  As in the test above: sensor 2's first
        # cell fails twice together, once in the regroup and once alone;
        # each of its later cells, a carry, once.
        self.outage(3, set(), refuse=2)
        skipped = self.logged(logging.WARNING, 'skipped for this pass')
        self.assertEqual(len(skipped), 7)
        self.assertIn("'S2' failed 4 times in a row; skipped", skipped[0])
        for message in skipped[1:]:
            self.assertIn("'S2' failed once; skipped", message)

    def test_a_gap_that_keeps_failing_takes_turns_with_a_kept_cell(self):
        # corr-r4-1.  A: an HT1 gap of 20 days at its 7d read.  The bucket
        # keeps 14 days, so its older cells are refused (422) on every call:
        # its first cell is kept for the grace day, and each later old cell
        # is a carry, given up after one failed call read alone.  B: a 2-day
        # HT1 gap that closed 22 h after A; for 3 hours from its 7d read
        # every re-read call fails, so its first cell is kept too.  Round 4
        # gave every carry 'never read alone': A's carries took every turn
        # for 33 h, and B's cell was given up at its grace end without a
        # call (119 of its 575 samples lost).  Now a kept cell takes every
        # other call read alone after the others, so A's crawl and B's kept
        # cell take turns.
        cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT1'})
        sensors = cloud.sensors()
        t0 = self.clock.t
        a_end = t0 - 7 * D + 60
        b_end = a_end + 22 * H
        spell = (b_end + 7 * D - 10 * M, b_end + 7 * D + 3 * H)
        b_calls = []
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            if ID_B in (sensors or ()):
                b_calls.append(self.clock.t)
            if spell[0] <= self.clock.t < spell[1]:
                raise ValueError('API request failed with status 502')
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        real_write = w.write

        def write(records):
            if any(spd._observed_epoch(r['time']) < self.clock.t - 14 * D
                   for r in records if r['measurement'] == 'SensorPush'):
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w, args=make_args(delay=10))
        cloud.add(ID_A, every(a_end - 21 * D, a_end, 5 * M) + every(a_end, a_end + 12 * D, 5 * M))
        d._register_hole(ID_A, a_end - 20 * D, a_end, sensors, now=a_end)
        d._recheck_holes[-1]['done'] = 4             # only the 7d read is left
        history = every(b_end - 2 * D + 5 * M, b_end - 5 * M, 5 * M)
        cloud.add(ID_B, every(b_end - 3 * D, b_end - 2 * D, 5 * M) + history
                  + every(b_end, b_end + 12 * D, 5 * M))
        d._register_hole(ID_B, b_end - 2 * D, b_end, sensors, now=b_end)
        hole_b = d._recheck_holes[-1]
        hole_b['done'] = 4
        while self.clock.t < t0 + 2 * D:
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        tag_b = spd.sensor_id_tag(ID_B)
        written = {spd._observed_epoch(r['time']) for batch in w.written for r in batch
                   if r['tags']['sensor_id'] == tag_b and r['measurement'] == 'SensorPush'}
        self.assertTrue(set(history) <= written)
        self.assertNotIn(hole_b, d._recheck_holes)
        # Its first cell failed 4 times in the spell: twice together, once
        # alone in turn, which kept it, and once in its first kept turn.
        # A's kept cell, whose records the bucket refuses, waits with the
        # others (load-r6-1), so B was the only cell of the kept kind; its
        # read alone in turn left the alternation as it was, and the call of
        # the alternation before it was A's, one of the others, so B's turn
        # came next, still in the spell.  (Round 6 counted A's cell as kept,
        # and A, read alone less recently, went first: 3 failed calls.)  The
        # next call for B, after A's, worked: at the same time as in round 6.
        self.assertEqual(len([t for t in b_calls if t < spell[1]]), 4)
        self.assertLess(min(t for t in b_calls if t >= spell[1]) - spell[1], 2 * H)
        self.assertEqual([m for m in self.logged(logging.WARNING, 'given up') if "'S1'" in m], [])

    def test_a_kept_cell_gets_its_turn_while_another_gap_crawls(self):
        # compat-r4-1.  K (HT1): a 2-h gap closed 7 days ago, at its 7d read,
        # whose late history is in the cloud; every call for it fails for 40
        # min, so its cell is kept.  X (HT.w): a gateway off for 40 days came
        # back 35 min ago, and the bucket keeps 30 days: the oldest of X's 88
        # cells are refused on every call, each one a carry after the first.
        # Round 4 read X's carries first, one per step: K was tried at 0, 5
        # and 20 min, then given up at its grace end without a call (9 of 9
        # samples lost).  Now K, a kept cell, takes every other call read
        # alone after the others, and has its turn within hours.
        K, X = ID_A, ID_B
        cloud = Cloud(self.clock, {K: 'HT1', X: 'HT.w'})
        sensors = cloud.sensors()
        t0 = self.clock.t
        k_end, x_end = t0 - 7 * D, t0 - 35 * M
        k_ok = []
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            if K in (sensors or ()):
                if self.clock.t < t0 + 40 * M:
                    raise ValueError('API request failed with status 502')
                k_ok.append(self.clock.t)
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        real_write = w.write

        def write(records):
            if any(spd._observed_epoch(r['time']) < self.clock.t - 30 * D for r in records):
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w, args=make_args(delay=10))
        history = every(k_end - 2 * H + 5 * M, k_end - 5 * M, 5 * M)
        cloud.add(K, every(k_end - 4 * H, k_end - 2 * H, 5 * M) + history
                  + every(k_end, t0 + 2 * D, 5 * M))
        d._register_hole(K, k_end - 2 * H, k_end, sensors, now=k_end)
        hole_k = d._recheck_holes[-1]
        hole_k['done'] = 4                           # only the 7d read is left
        cloud.add(X, every(x_end - 40 * D - 2 * H, t0 + 2 * D, 10 * M))
        d._register_hole(X, x_end - 40 * D, x_end, sensors, now=x_end)
        self.assertEqual(spd._hole_ncells(d._recheck_holes[-1]), 88)
        grace_end = k_end + 7 * D + spd.RECHECK_LAST_PASS_GRACE
        while self.clock.t < grace_end + H:
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        self.assertEqual(len(self.logged(logging.WARNING, 'this is its last re-read')), 1)
        self.assertNotIn(hole_k, d._recheck_holes)
        self.assertTrue(set(history) <= set(written_times(w)))
        self.assertEqual(len(k_ok), 1)
        self.assertLess(k_ok[0] - t0, 3 * H)
        # X's crawl goes on meanwhile: many of its old cells were given a
        # call and skipped.
        self.assertGreater(len(self.logged(logging.WARNING, "'S1' failed once; skipped")), 15)

    def test_carries_after_a_grace_end_hold_up_no_kept_cell(self):
        # load-r4-2.  X (HT1): a 20-day gap at its 7d read whose cells older
        # than the bucket's 14 days are refused; its grace ends at t0 + 1 d,
        # and from then on each such cell is a carry, given up after one call
        # read alone.  K (HT.w): a 1-day gap at its 7d read, due at t0 + 26 h;
        # its first 3 calls fail, so its cell is kept until t0 + 50 h.  C
        # (HT.w): a 1-day gap noted at t0 + 28 h whose first 3 calls fail: its
        # first cell is skipped and the next is a carry.  B (HT.w): a new 1-h
        # gap every 2 h.  Round 4 read X's carries first until t0 + 58 h: K's
        # cell was given up at its grace end without a call (599 of 1439
        # samples lost), and C's carry waited 30 h.  Round 5 had the gaps
        # take turns, which kept every crawl behind a bucket's retention
        # (crawl-r5).  Now a kept cell takes every other call read alone
        # after the others, and the others go one gap at a time: K is read
        # one step after its third failure; C's carry is no kept cell, so it
        # waits for X's crawl, as in round 4, and loses nothing - its first
        # cell, skipped, is read by its 3d read (the 6h and 1d reads went by
        # while it waited).  The run lasts until after that read.
        X, K, C, B = ID_A, ID_B, ID_C, '300777.4567890123456789'
        cloud = Cloud(self.clock, {X: 'HT1', K: 'HT.w', C: 'HT.w', B: 'HT.w'})
        sensors = cloud.sensors()
        t0 = self.clock.t
        left = {K: 3, C: 3}
        calls = []                                   # (time, sensors, outcome)
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            s = tuple(sensors or ())
            for sid in (K, C):
                if sid in s and left[sid]:
                    left[sid] -= 1
                    calls.append((self.clock.t, s, 'failed'))
                    raise ValueError('API request failed with status 502')
            calls.append((self.clock.t, s, 'read'))
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        real_write = w.write

        def write(records):
            if any(spd._observed_epoch(r['time']) < self.clock.t - 14 * D for r in records):
                calls[-1] = calls[-1][:2] + ('refused',)
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w)
        x_end = t0 - 7 * D
        cloud.add(X, every(x_end - 20 * D - 2 * H, x_end + 2 * H, 10 * M))
        d._register_hole(X, x_end - 20 * D, x_end, sensors, now=x_end)
        d._recheck_holes[-1]['done'] = 4
        k_end = t0 + 26 * H - 7 * D
        cloud.add(K, every(k_end - D - 2 * H, k_end + 2 * H))
        d._register_hole(K, k_end - D, k_end, sensors, now=k_end)
        hole_k = d._recheck_holes[-1]
        hole_k['done'] = 4
        k_grace_end = k_end + 7 * D + spd.RECHECK_LAST_PASS_GRACE
        cloud.add(C, every(t0 + 26 * H - D - 2 * H, t0 + 4 * D))
        cloud.add(B, every(t0 - 2 * H, t0 + 4 * D))
        c_hole, next_b, k_gone = None, t0 + 10 * M, None
        while self.clock.t < t0 + 101 * H:
            w._consecutive_failures = 0              # the cycle's poll landed
            if self.clock.t >= next_b:
                d._register_hole(B, self.clock.t - 80 * M, self.clock.t - 20 * M,
                                 sensors, now=self.clock.t)
                next_b += 2 * H
            if c_hole is None and self.clock.t >= t0 + 28 * H:
                d._register_hole(C, self.clock.t - 25 * H, self.clock.t - 20 * M,
                                 sensors, now=self.clock.t)
                c_hole = d._recheck_holes[-1]
            d._recheck_gaps(sensors, 'SensorPush')
            if k_gone is None and hole_k not in d._recheck_holes:
                k_gone = self.clock.t
            self.clock.advance(5 * M)

        def times(sid, outcome):
            return [t for t, s, r in calls if sid in s and r == outcome]

        def written(sid):
            tag = spd.sensor_id_tag(sid)
            return {spd._observed_epoch(r['time']) for batch in w.written for r in batch
                    if r['tags']['sensor_id'] == tag and r['measurement'] == 'SensorPush'}
        # K: read one step of the alone back-off after its third failure, in
        # full.
        k_failed = times(K, 'failed')
        self.assertEqual(len(k_failed), 3)
        self.assertLess(times(K, 'read')[0] - k_failed[-1], 2 * H + 5 * M)
        self.assertLess(k_gone, k_grace_end)
        self.assertTrue({t for t in cloud.samples[K] if k_end - D < t < k_end} <= written(K))
        self.assertEqual([m for m in self.logged(logging.WARNING, 'given up') if "'S1'" in m], [])
        # C: its carry waits for X's crawl, at most as long as in round 4
        # (108,745 s, 30.2 h), and its whole history is written.
        self.assertEqual(len(self.logged(logging.WARNING, "'S2' failed 3 times in a row; skipped")), 1)
        self.assertLessEqual(times(C, 'read')[0] - times(C, 'failed')[-1], 108745)
        self.assertNotIn(c_hole, d._recheck_holes)
        self.assertTrue({t for t in cloud.samples[C]
                         if c_hole['start'] < t < c_hole['end']} <= written(C))
        # X: the calls of round 4, 61, and the one its kept cell gets after
        # its grace end; at most 28 a day, the most round 4 made in a day.
        x_failed = [t for t, s, r in calls if X in s and r != 'read']
        self.assertEqual(len(x_failed), 62)
        self.assertLessEqual(max(len([t for t in x_failed if t0 + k * D <= t < t0 + (k + 1) * D])
                                 for k in range(3)), 28)
        self.assertEqual(len(self.logged(logging.WARNING, 'Gap re-reads pause')), 6)

    def test_crawls_through_cells_a_bucket_refuses_go_one_after_another(self):
        # crawl-r5.  12 HT1 sensors were off for 8 days and came back 30 min
        # ago with their history.  The bucket keeps 7 days and refuses every
        # batch that holds an older point (422, as influxdb-client raises),
        # so the oldest cells of every hole fail on every call: each hole
        # crawls through them, one failed call read alone per cell, on the
        # alone back-off (about one an hour).  Round 5 had the holes take
        # turns, so each crawl moved two cells a day, slower than the
        # retention boundary: none reached its writable cells, and none of
        # the 24,048 samples the bucket still took at the return was
        # written in 3 days.  The crawls now go one after another, each as
        # fast as the back-off allows (round 4: 21,852 written).
        kinds = {'%d.1234567890123456' % (500000 + k): 'HT1' for k in range(12)}
        cloud = Cloud(self.clock, kinds)
        sensors = cloud.sensors()
        t0 = self.clock.t
        back = t0 - 30 * M
        off = back - 8 * D
        history = every(off + 5 * M, back - 5 * M, 5 * M)
        for sid in kinds:
            cloud.add(sid, every(off - 2 * H, off, 5 * M) + history + every(back, t0 + H, 5 * M))
        w = fake_writer()
        real_write = w.write

        def write(records):
            if any(spd._observed_epoch(r['time']) < self.clock.t - 7 * D
                   for r in records if r['measurement'] == 'SensorPush'):
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w, args=make_args(delay=10))
        for sid in kinds:
            d._register_hole(sid, off, back, sensors, now=back)
        while self.clock.t < t0 + 3 * D:
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        # What the bucket still took at the return, an hour inside its
        # retention.
        writable = [t for t in history if t >= back - 7 * D + H]
        self.assertEqual(len(writable) * len(kinds), 24048)
        sid_of = {spd.sensor_id_tag(sid): sid for sid in kinds}
        got = {(sid_of[r['tags']['sensor_id']], spd._observed_epoch(r['time']))
               for batch in w.written for r in batch if r['measurement'] == 'SensorPush'}
        wrote = sum((sid, t) in got for sid in kinds for t in writable)
        self.assertGreater(wrote, 0.9 * 24048)

    def test_a_kept_cell_takes_every_other_call_while_a_gap_crawls(self):
        # crawl-r5.  X1 and X2 (HT.w): one gateway, off for 20 days, back 35
        # min ago.  The bucket keeps 14 days, so the oldest cells of both
        # holes are refused on every call; X1's hole crawls through them,
        # one failed call read alone per cell, and X2's waits for it (gaps
        # that keep failing go one at a time).  K (HT.w): a 2-h gap at its
        # 7d read, due 3 h in, whose calls fail until 12 h in (a 502): its
        # cell is kept.  Round 5 had the three gaps take turns, so K got
        # every third call read alone.  Now K takes every other one, and the
        # crawl goes on between K's calls.  Once, a poll write fails just
        # before the crawl's turn: the writers gate stops that pick, and it
        # costs the crawl no turn - the alternation counts calls made.
        X1, X2, K = ID_A, ID_B, ID_C
        cloud = Cloud(self.clock, {X1: 'HT.w', X2: 'HT.w', K: 'HT.w'})
        sensors = cloud.sensors()
        t0 = self.clock.t
        x_end, k_end = t0 - 35 * M, t0 + 3 * H - 7 * D
        calls = []                                   # (sensors, outcome, start)
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            s = tuple(sensors or ())
            if K in s and self.clock.t < t0 + 12 * H:
                calls.append((s, 'failed', start))
                raise ValueError('API request failed with status 502')
            calls.append((s, 'read', start))
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        real_write = w.write

        def write(records):
            if any(spd._observed_epoch(r['time']) < self.clock.t - 14 * D for r in records):
                calls[-1] = (calls[-1][0], 'refused', calls[-1][2])
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w)
        for sid in (X1, X2):
            cloud.add(sid, every(x_end - 20 * D - 2 * H, t0 + D, 10 * M))
            d._register_hole(sid, x_end - 20 * D, x_end, sensors, now=x_end)
        history = every(k_end - 2 * H + 5 * M, k_end - 5 * M, 5 * M)
        cloud.add(K, every(k_end - 4 * H, k_end - 2 * H, 5 * M) + history
                  + every(k_end, t0 + D, 5 * M))
        d._register_hole(K, k_end - 2 * H, k_end, sensors, now=k_end)
        d._recheck_holes[-1]['done'] = 4             # only the 7d read is left
        stopped = None
        while self.clock.t < t0 + 14 * H:
            w._consecutive_failures = 0              # the cycle's poll landed
            k_failed = [i for i, (s, r, _) in enumerate(calls) if s == (K,) and r == 'failed']
            if (stopped is None and len(k_failed) >= 3 and len(calls) == k_failed[2] + 4
                    and d._recheck_alone_next_at is not None
                    and self.clock.mono >= d._recheck_alone_next_at):
                # K, X1 and K were read alone since K's cell was kept; the
                # next read alone would be X1's, but this poll failed.
                stopped = len(calls)
                w._consecutive_failures = 1
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        self.assertEqual(len(self.logged(logging.WARNING, 'gap re-read(s) wait')), 1)
        self.assertEqual(calls[stopped][0], (X1,))
        k_failed = [i for i, (s, r, _) in enumerate(calls) if s == (K,) and r == 'failed']
        k_read = next(i for i, (s, r, _) in enumerate(calls) if s == (K,) and r == 'read')
        # Two failed calls read together, the third alone: from then on K's
        # cell is kept, and every call is read alone.
        self.assertEqual(len(self.logged(logging.WARNING, 'this is its last re-read')), 1)
        after = calls[k_failed[2] + 1:k_read + 1]
        self.assertGreater(len(after), 6)
        # K and X1's crawl take every other call; X2 waits for the crawl.
        # The read alone that kept K's cell came in turn, as the first read
        # alone of its window, and is not one of them; the last before it
        # was X1's, so K's comes first.
        self.assertEqual([s for s, _, _ in after],
                         [(K,), (X1,)] * (len(after) // 2) + [(K,)] * (len(after) % 2))
        self.assertEqual(after[-1][0], (K,))
        # Each of X1's calls was refused and moved its crawl on by one cell.
        x1 = [(r, start) for s, r, start in after if s == (X1,)]
        self.assertEqual({r for r, _ in x1}, {'refused'})
        starts = [spd._observed_epoch(start) for _, start in x1]
        self.assertEqual(starts, sorted(set(starts)))
        self.assertTrue(set(history) <= set(written_times(w)))

    def crawl_and_kept(self, x_days, retention, k_due, k_write=None, k_api=None,
                       until=None, k_len=2 * H):
        """X (HT.w): a gateway off for ``x_days`` came back 35 min ago.  The
        bucket keeps ``retention`` and refuses (422) every batch with an older
        point, so X's passes crawl through its oldest cells, one failed call
        read alone per cell.  K (HT.w): a gap of ``k_len`` (2 h) at its 7d
        read, due ``k_due`` after t0, with its history in the cloud.  Writes
        with K's records also fail within ``k_write`` (a backend refusing
        them for a while), and calls with K within ``k_api`` (a 502).  5-min
        cycles until ``until`` or until K's gap is gone.  Returns the calls
        (time, sensors, outcome, window start, read alone), K's hole,
        history and grace end."""
        X, K = ID_A, ID_B
        cloud = Cloud(self.clock, {X: 'HT.w', K: 'HT.w'})
        sensors = cloud.sensors()
        t0 = self.clock.t
        x_end, k_end = t0 - 35 * M, t0 + k_due - 7 * D
        calls, alone = [], [False]
        real_get = cloud.get_samples

        def within(span):
            return span is not None and t0 + span[0] <= self.clock.t < t0 + span[1]

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            s = tuple(sensors or ())
            if K in s and within(k_api):
                calls.append((self.clock.t, s, 'failed', start, alone[0]))
                raise ValueError('API request failed with status 502')
            calls.append((self.clock.t, s, 'read', start, alone[0]))
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        real_write = w.write
        tag_k = spd.sensor_id_tag(K)

        def write(records):
            old = any(spd._observed_epoch(r['time']) < self.clock.t - retention
                      for r in records if r['measurement'] == 'SensorPush')
            if old or (within(k_write) and any(r['tags']['sensor_id'] == tag_k
                                               for r in records)):
                calls[-1] = calls[-1][:2] + ('refused',) + calls[-1][3:]
                raise OSError('422: points beyond retention policy dropped' if old
                              else '503: service unavailable')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w, args=make_args(delay=10))
        real_call = d._recheck_call

        def recheck_call(batch, *a, **k):
            # Only cells that failed twice at least are read alone.
            alone[0] = d._cell_fails(batch[0]) >= 2
            return real_call(batch, *a, **k)
        d._recheck_call = recheck_call
        cloud.add(X, every(x_end - x_days - 2 * H, t0 + 3 * D, 10 * M))
        d._register_hole(X, x_end - x_days, x_end, sensors, now=x_end)
        history = every(k_end - k_len + 5 * M, k_end - 5 * M, 5 * M)
        cloud.add(K, every(k_end - k_len - 2 * H, k_end - k_len, 5 * M) + history
                  + every(k_end, t0 + 3 * D, 5 * M))
        d._register_hole(K, k_end - k_len, k_end, sensors, now=k_end)
        hole_k = d._recheck_holes[-1]
        hole_k['done'] = 4                           # only the 7d read is left
        grace_end = k_end + 7 * D + spd.RECHECK_LAST_PASS_GRACE
        while hole_k in d._recheck_holes and (until is None or self.clock.t < t0 + until):
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        return {'calls': calls, 'hole_k': hole_k, 'history': history, 'w': w,
                'grace_end': grace_end, 'd': d, 'X': X, 'K': K}

    def test_a_refused_kept_cell_waits_while_a_gap_crawls(self):
        # load-r6-1.  The bucket keeps 7 days, so K's 7d read is refused
        # outright: its records are all older than that.  Its cell is kept on
        # its third failed call.  X's gap (20 days) crawls meanwhile through
        # its cells older than 7 days.  Round 6 gave K every other call read
        # alone for its grace day: tries that could never work, and X's crawl
        # moved one cell per two steps of the back-off while X's history aged
        # out of the bucket.  Now a cell whose last failed call a backend
        # refused waits with the others: X's crawl keeps every read alone,
        # and K still gets one call after its grace end, which gives it up -
        # no cell is given up without a call.  K's gap is one cell, so that
        # call only gives it up: it waits for X's crawl to end also when K
        # is the more overdue gap (due 3.5 h before X's 1h read).
        for k_due in (3 * H, -3.5 * H):
            with self.subTest(k_due=k_due):
                self.reset_state()
                del self.records[:]
                run = self.crawl_and_kept(20 * D, 7 * D, k_due)
                calls, X, K = run['calls'], run['X'], run['K']
                k_calls = [i for i, c in enumerate(calls) if K in c[1]]
                # Twice together, once alone in turn (kept), and once after
                # the grace end; every one refused.
                self.assertEqual(len(k_calls), 4)
                self.assertEqual({calls[i][2] for i in k_calls}, {'refused'})
                self.assertLess(calls[k_calls[2]][0], run['grace_end'])
                self.assertGreater(calls[k_calls[3]][0], run['grace_end'])
                given_up = self.logged(logging.WARNING, 'its last re-read is given up')
                self.assertEqual(len(given_up), 1)
                self.assertIn("'S1' failed 4 times in a row", given_up[0])
                self.assertNotIn(run['hole_k'], run['d']._recheck_holes)
                # Between K's third and fourth call, every call was X's: its
                # crawl, one refused call read alone per cell, for more than
                # K's grace day, until its first read that worked; then the
                # rest of its pass.
                between = calls[k_calls[2] + 1:k_calls[3]]
                self.assertEqual({c[1] for c in between}, {(X,)})
                self.assertIn('read', {c[2] for c in between})
                refused = [c for c in between if c[2] == 'refused' and c[4]]
                self.assertGreater(len(refused), 20)
                starts = [spd._observed_epoch(c[3]) for c in refused]
                self.assertEqual(starts, sorted(set(starts)))
                self.assertGreater(refused[-1][0], run['grace_end'])

    def test_a_kept_cell_takes_its_turn_again_after_a_failure_at_the_api(self):
        # load-r6-1.  The bucket keeps 14 days.  K's 7d read is due at once,
        # but for 2 hours a backend refuses every write with K's records, so
        # its cell is kept on its third failed call, refused.  It waits with
        # the others while X's 1h pass crawls through X's cells older than
        # 14 days.  Then every call with K fails (a 502) until 12 h in: K's
        # next call, after X's crawl, fails at the API, and gives K its kept
        # turn back.  In X's 6h pass, which crawls again, K and X take every
        # other call read alone, until K's read works.
        run = self.crawl_and_kept(15 * D, 14 * D, 5 * M, k_write=(0, 2 * H),
                                  k_api=(2 * H, 12 * H), until=D)
        calls, X, K = run['calls'], run['X'], run['K']
        k_calls = [i for i, c in enumerate(calls) if K in c[1]]
        self.assertEqual([calls[i][2] for i in k_calls[:3]], ['refused'] * 3)
        # While X's 1h pass crawled, until its first read that worked, no
        # call was K's.  (Round 6 gave K every other call read alone.)
        x_ok = next(i for i, c in enumerate(calls) if c[1] == (X,) and c[2] == 'read')
        self.assertTrue(k_calls[2] < x_ok < k_calls[3])
        crawl = calls[k_calls[2] + 1:x_ok]
        self.assertGreater(len([c for c in crawl if c[4]]), 2)
        self.assertEqual({(c[1], c[2]) for c in crawl}, {((X,), 'refused')})
        self.assertEqual(calls[k_calls[3]][2], 'failed')
        # X's 6h pass crawls again.  After its first read alone, which comes
        # in turn, its crawl and K take every other call read alone, until
        # the crawl's first read that works.
        x_alone = [i for i in range(x_ok + 1, len(calls))
                   if calls[i][1] == (X,) and calls[i][4]]
        x_ok2 = next(i for i in x_alone if calls[i][2] == 'read')
        turns = [calls[i][1] for i in range(x_alone[1], x_ok2 + 1) if calls[i][4]]
        self.assertGreaterEqual(len(turns), 5)
        self.assertEqual(turns, [(X,), (K,)] * (len(turns) // 2) + [(X,)])
        self.assertEqual({calls[i][2] for i in k_calls[3:-1]}, {'failed'})
        self.assertEqual(calls[k_calls[-1]][2], 'read')
        self.assertTrue(set(run['history']) <= set(written_times(run['w'])))
        self.assertNotIn(run['hole_k'], run['d']._recheck_holes)

    def test_past_its_grace_end_a_refused_kept_cell_goes_in_its_gaps_turn(self):
        # load-r6-1.  The bucket keeps 14 days.  K: a 10-day gap at its 7d
        # read, due now: its oldest cells are refused, so its first cell is
        # kept, refused, and waits with the others for its grace day, while
        # X's gap (30 days, back 35 min ago) crawls through its cells older
        # than 14 days.  Past its grace end K's next failed call gives it
        # up, so it counts as never read alone, as the carry that round 4
        # made of it at the grace end: more overdue than X's crawl, it is
        # given up at once, and K's own crawl goes on, ahead of X's.  Sorted
        # as read alone, it waited until X's crawl ended, while K's history
        # aged out of the bucket (parallel_crawl, 12 sensors, 21-day outage:
        # 48.7 % written instead of 54.2 %).
        run = self.crawl_and_kept(30 * D, 14 * D, 0, k_len=10 * D, until=40 * H)
        calls, X, K, grace_end = run['calls'], run['X'], run['K'], run['grace_end']
        k_calls = [i for i, c in enumerate(calls) if K in c[1]]
        # Twice together, once alone in turn, which kept it: all refused.
        self.assertEqual([calls[i][2] for i in k_calls[:3]], ['refused'] * 3)
        self.assertGreater(len(k_calls), 3)
        # No call for K in its grace day after that: X's crawl had them.
        between = calls[k_calls[2] + 1:k_calls[3]]
        self.assertGreater(len(between), 20)
        self.assertEqual({c[1] for c in between}, {(X,)})
        # Past its grace end, at the next step of the back-off.
        self.assertGreater(calls[k_calls[3]][0], grace_end)
        self.assertLessEqual(calls[k_calls[3]][0] - grace_end, H + 5 * M)
        given_up = self.logged(logging.WARNING, 'its last re-read is given up')
        self.assertIn("'S1' failed 4 times in a row", given_up[0])
        # It came in the middle of X's crawl, and K's crawl followed: only
        # K's calls read alone, until K's first read that works.  X's crawl
        # was not over then: its 1h pass is still under way.
        self.assertEqual(calls[k_calls[3] - 1][1:3], ((X,), 'refused'))
        k_ok = next(i for i in k_calls if calls[i][2] == 'read')
        crawl = calls[k_calls[3]:k_ok + 1]
        self.assertGreater(len(crawl), 3)
        self.assertTrue(all(c[1] == (K,) and c[4] for c in crawl))
        self.assertEqual([h['done'] for h in run['d']._recheck_holes if h['sid'] == X], [0])

    def test_past_its_grace_end_a_kept_cell_that_failed_at_the_api_keeps_its_turn(self):
        # load-r7-1.  Four HT1 gaps at their 7d read, due an hour apart from
        # 30 min in, whose late history is in the cloud: G0 of 14 h (2
        # cells), G1 to G3 of 2 h.  Every re-read call fails at the API (a
        # 502) for 27 h, so each gap's first cell is kept and tried in its
        # turn, one call per step of the alone back-off.  Round 7 counted
        # G0's kept cell as never read alone once its grace end had passed,
        # as it does for a cell whose records a backend refused: it went
        # ahead of the kept cells still in their grace day, and its call
        # came at the next step of the back-off, while the API still failed.
        # G0 was given up, then its carry: 69 of the 236 samples were
        # written.  A kept cell whose last call failed at the API keeps its
        # turn, the least recently read alone first: G1 and G2 get their
        # tries in their grace day, and G0's next call comes after the
        # spell, and works.
        ids = ['%d.1234567890123456' % (470000 + k) for k in range(4)]
        cloud = Cloud(self.clock, dict.fromkeys(ids, 'HT1'))
        sensors = cloud.sensors()
        t0 = self.clock.t
        spell_end = t0 + 27 * H
        calls = []                                   # (time, sensors, outcome)
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            s = tuple(sensors or ())
            if self.clock.t < spell_end:
                calls.append((self.clock.t, s, 'failed'))
                raise ValueError('API request failed with status 502')
            calls.append((self.clock.t, s, 'read'))
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        d = self.daemon(cloud, writer=w, args=make_args(delay=10, interval=300))
        history, holes = set(), []
        for k, sid in enumerate(ids):
            end = t0 + 30 * M + k * H - 7 * D
            length = (14 if k == 0 else 2) * H
            late = every(end - length + 5 * M, end - 5 * M, 5 * M)
            history |= {(spd.sensor_id_tag(sid), t) for t in late}
            cloud.add(sid, every(end - length - 3 * H, end - length, 5 * M) + late
                      + every(end, t0 + 5 * D, 5 * M))
            d._register_hole(sid, end - length, end, sensors, now=end)
            holes.append(d._recheck_holes[-1])
            holes[-1]['done'] = 4                    # only the 7d read is left
        self.assertEqual([spd._hole_ncells(h) for h in holes], [2, 1, 1, 1])
        while any(h in d._recheck_holes for h in holes) and self.clock.t < t0 + 4 * D:
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        self.assertFalse(any(h in d._recheck_holes for h in holes))
        written = {(r['tags']['sensor_id'], spd._observed_epoch(r['time']))
                   for batch in w.written for r in batch if r['measurement'] == 'SensorPush'}
        self.assertEqual(len(history), 236)
        self.assertEqual(len(history & written), 236)
        self.assertEqual(self.logged(logging.WARNING, 'given up'), [])
        # Each gap's last failed call came in its grace day, and its next
        # call, after the spell, read it.
        for sid, hole in zip(ids, holes):
            grace_end = hole['closed_at'] + 7 * D + spd.RECHECK_LAST_PASS_GRACE
            mine = [(t, r) for t, s, r in calls if sid in s]
            self.assertLess(max(t for t, r in mine if r == 'failed'), grace_end)
            self.assertEqual({r for t, r in mine if t >= spell_end}, {'read'})

    def test_a_window_demoted_on_its_last_read_gets_its_tries_while_a_gap_crawls(self):
        # corr-r6-1.  X (HT.w): a gateway off for 13 days came back 35 min
        # ago, and the backend refuses every write with X's records, so X's
        # 1h pass crawls through all of its 29 cells, one failed call read
        # alone each, for more than a day.  C and D (HT1): one 6-h gap at its
        # 7d read, due 2 h in, whose late history is in the cloud.  From 10
        # min before that read until 3 h after it every call with C or D
        # fails (a 502): their batch fails twice, C's read alone fails and
        # keeps C's cell, and D's window is no longer in turn.  From D's
        # grace end on, every call with D fails for a day.  Round 6 sorted
        # D's window with the carries: behind X's crawl it got no try in its
        # grace day, and its single call, after the grace end, failed: all of
        # D's history was lost.  A window demoted on its last pass is its
        # cell's last read, like a kept cell, so it takes the kept turns.
        X, C, Dd = ID_A, ID_B, ID_C
        cloud = Cloud(self.clock, {X: 'HT.w', C: 'HT1', Dd: 'HT1'})
        sensors = cloud.sensors()
        t0 = self.clock.t
        x_end, cd_end = t0 - 35 * M, t0 + 2 * H - 7 * D
        spell1 = (cd_end + 7 * D - 10 * M, cd_end + 7 * D + 3 * H)
        grace_end = cd_end + 7 * D + spd.RECHECK_LAST_PASS_GRACE
        spell2 = (grace_end, grace_end + D)
        calls = []                                   # (time, sensors, outcome)
        real_get = cloud.get_samples

        def get_samples(start, stop, measures=None, limit=0, sensors=None):
            s, now = tuple(sensors or ()), self.clock.t
            if ((C in s or Dd in s) and spell1[0] <= now < spell1[1]
                    or Dd in s and spell2[0] <= now < spell2[1]):
                calls.append((now, s, 'failed'))
                raise ValueError('API request failed with status 502')
            calls.append((now, s, 'read'))
            return real_get(start, stop, measures, limit, sensors)
        cloud.get_samples = get_samples
        w = fake_writer()
        real_write = w.write
        tag_x = spd.sensor_id_tag(X)

        def write(records):
            if any(r['tags']['sensor_id'] == tag_x for r in records):
                raise OSError('422: points beyond retention policy dropped')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w, args=make_args(delay=10))
        cloud.add(X, every(x_end - 13 * D - 2 * H, t0 + 3 * D, 10 * M))
        d._register_hole(X, x_end - 13 * D, x_end, sensors, now=x_end)
        self.assertEqual(spd._hole_ncells(d._recheck_holes[-1]), 29)
        history, holes = {}, []
        for sid in (C, Dd):
            history[sid] = every(cd_end - 6 * H + 5 * M, cd_end - 5 * M, 5 * M)
            cloud.add(sid, every(cd_end - 8 * H, cd_end - 6 * H, 5 * M) + history[sid]
                      + every(cd_end, t0 + 3 * D, 5 * M))
            d._register_hole(sid, cd_end - 6 * H, cd_end, sensors, now=cd_end)
            holes.append(d._recheck_holes[-1])
            holes[-1]['done'] = 4                    # only the 7d read is left
        while (any(h in d._recheck_holes for h in holes)
               and self.clock.t < spell2[1] + H):
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        self.assertEqual(self.logged(logging.WARNING, 'given up'), [])
        self.assertFalse(any(h in d._recheck_holes for h in holes))
        got = {}
        for batch in w.written:
            for r in batch:
                got.setdefault(r['tags']['sensor_id'], set()).add(
                    spd._observed_epoch(r['time']))
        for sid in (C, Dd):
            self.assertTrue(set(history[sid]) <= got[spd.sensor_id_tag(sid)])
        # D: twice together in spell 1, then read alone in its grace day;
        # X's crawl went on meanwhile.
        d_calls = [(t, s, r) for t, s, r in calls if Dd in s]
        self.assertEqual([len(s) for _, s, _ in d_calls[:2]], [2, 2])
        self.assertEqual(d_calls[2][1], (Dd,))
        self.assertLess(d_calls[2][0], spell1[1])
        self.assertLess(d_calls[-1][0], grace_end)
        self.assertEqual(d_calls[-1][2], 'read')
        self.assertGreater(len([c for c in calls if c[1] == (X,) and c[0] < d_calls[-1][0]]), 3)

    def test_a_kept_cell_past_its_grace_end_gets_one_more_call(self):
        # corr-r4-1.  A kept cell is given up only by a call that fails after
        # its grace end, never by the clock alone.  Round 4 gave it up at its
        # grace end without a call, however long ago it was last tried.  Here
        # an HT1 gap's 7d read fails until its grace end; the first call after
        # it fails (the cell is given up) or works (its history is written).
        for works in (False, True):
            with self.subTest(works=works):
                self.reset_state()
                del self.records[:]
                cloud = Cloud(self.clock, {ID_A: 'HT1'})
                w = fake_writer()
                d = self.daemon(cloud, writer=w)
                t0 = self.clock.t
                a, b = t0 - 7 * D - 3 * H + 60, t0 - 7 * D + 60
                history = every(a + 5 * M, b - 5 * M, 5 * M)
                cloud.add(ID_A, every(a - H, a, 5 * M) + history + every(b, t0 + 3 * D, 5 * M))
                d._register_hole(ID_A, a, b, cloud.sensors(), now=b)
                d._recheck_holes[0]['done'] = 4      # only the 7d read is left
                grace_end = b + 7 * D + spd.RECHECK_LAST_PASS_GRACE
                calls = []
                real_get = cloud.get_samples

                def get_samples(start, stop, measures=None, limit=0, sensors=None,
                                calls=calls, real_get=real_get, grace_end=grace_end,
                                works=works):
                    calls.append(self.clock.t)
                    if self.clock.t < grace_end or not works:
                        raise ValueError('API request failed with status 500')
                    return real_get(start, stop, measures, limit, sensors)
                cloud.get_samples = get_samples
                while self.clock.t < grace_end + 3 * H:
                    d._recheck_gaps(cloud.sensors(), 'SensorPush')
                    self.clock.advance(5 * M)
                self.assertEqual(len([t for t in calls if t > grace_end]), 1)
                self.assertEqual(d._recheck_holes, [])
                given_up = self.logged(logging.WARNING, 'its last re-read is given up')
                if works:
                    self.assertTrue(set(history) <= set(written_times(w)))
                    self.assertEqual(given_up, [])
                else:
                    self.assertEqual(written_times(w), [])
                    self.assertEqual(len(given_up), 1)
                    self.assertIn('failed %d times in a row' % len(calls), given_up[0])

    def test_no_call_after_a_read_alone_whose_write_failed(self):
        # load-r4-3.  A's window failed twice (a 502); at its read alone the
        # backend stops taking writes.  Round 4 went on with B's call
        # although its answer could not be written either: one more API
        # call, a failure counted against B's cell and one more step of the
        # common back-off.  The writers gate is now asked before every call.
        cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT.w'})
        sensors = cloud.sensors()
        w = fake_writer()
        down = [False]
        real_write = w.write

        def write(records):
            if down[0]:
                raise OSError('connection refused')
            real_write(records)
        w.write = write
        d = self.daemon(cloud, writer=w)
        t0 = self.clock.t
        a_end = t0 - 7 * D
        cloud.add(ID_A, every(a_end - 5 * H, a_end + H))
        d._register_hole(ID_A, a_end - 4 * H, a_end - 3 * H, sensors, now=a_end)
        hole_a = d._recheck_holes[-1]
        hole_a['done'] = 4                           # only its 7d read is left
        cloud.add(ID_B, every(t0 - 5 * H, t0 + D))
        cloud.fail = 2
        for _ in range(3):
            w._consecutive_failures = 0              # the cycle's poll landed
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        self.assertEqual((hole_a['_fail']['n'], d._recheck_fail_streak), (2, 2))
        # B: a gap due at once, once A's pause has ended.
        d._register_hole(ID_B, self.clock.t - 80 * M, self.clock.t - 20 * M,
                         sensors, now=self.clock.t - H)
        hole_b = d._recheck_holes[-1]
        while self.clock.mono < d._recheck_next_at:
            self.clock.advance(5 * M)
        down[0] = True
        w._consecutive_failures = 0
        n0 = len(cloud.sample_calls())
        d._recheck_gaps(sensors, 'SensorPush')
        self.assertEqual([c[4] for c in cloud.sample_calls()[n0:]], [(ID_A,)])
        self.assertNotIn('_fail', hole_b)
        self.assertEqual(d._recheck_fail_streak, 2)
        self.assertEqual(len(self.logged(
            logging.WARNING, 'wait: %s failed its last write' % type(w).__name__)), 1)
        # The next cycle's poll lands: B is read.
        down[0] = False
        w._consecutive_failures = 0
        self.clock.advance(5 * M)
        d._recheck_gaps(sensors, 'SensorPush')
        self.assertEqual(hole_b['done'], 1)
        self.assertEqual(len(self.logged(logging.INFO, 'gap re-reads resume')), 1)

    def test_a_read_alone_that_works_resets_the_common_streak(self):
        # load-r4-4.  A's calls fail twice together, then its read alone
        # works.  Round 4 reset the common streak only when a call read
        # together worked, so it stayed at 2, and one failed call of another
        # gap a day later paused every re-read for 35 min, 'after 3 failed
        # call(s) in a row'.  Any call that works resets it now.
        cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_C: 'HT.w'})
        sensors = cloud.sensors()
        d = self.daemon(cloud)
        t0 = self.clock.t
        a_end = t0 - 7 * D
        cloud.add(ID_A, every(a_end - 5 * H, a_end + H) + every(t0 - 5 * H, t0 + 3 * D))
        cloud.add(ID_C, every(t0 - 5 * H, t0 + 3 * D))
        d._register_hole(ID_A, a_end - 4 * H, a_end - 3 * H, sensors, now=a_end)
        d._recheck_holes[-1]['done'] = 4             # only its 7d read is left
        cloud.fail = 2
        for _ in range(12):
            d._recheck_gaps(sensors, 'SensorPush')
            self.clock.advance(5 * M)
        self.assertEqual(len(cloud.sample_calls()), 3)
        self.assertEqual((d._recheck_holes, d._recheck_fail_streak), ([], 0))
        self.clock.advance(D)
        d._register_hole(ID_C, self.clock.t - 40 * M, self.clock.t - 20 * M,
                         sensors, now=self.clock.t)
        self.clock.advance(H)
        cloud.fail = 1
        d._recheck_gaps(sensors, 'SensorPush')
        # A's two failures, then C's: one step (INTERVAL x 1), as the first.
        self.assertEqual(self.logged(logging.WARNING, 'Gap re-reads pause'), [
            'Gap re-reads pause for 5 min after 1 failed call(s) in a row',
            'Gap re-reads pause for 15 min after 2 failed call(s) in a row',
            'Gap re-reads pause for 5 min after 1 failed call(s) in a row'])
        self.assertEqual(d._recheck_next_at - self.clock.mono, 5 * M)


class TwoBackends(RecheckCase):
    """Re-read samples are older than what a backend already holds, so a
    backend that misses them never gets them from the polls."""

    def setUp(self):
        super().setUp()
        self.cloud = Cloud(self.clock, {ID_B: 'HT.w'})
        self.sensors = self.cloud.sensors()
        self.wa, self.wb = fake_writer(), fake_writer()
        self.d = self.make_daemon([self.wa, self.wb], api=self.cloud)
        self.d._interruptible_sleep = self.sleep
        t0 = self.clock.t
        a, b = t0 - 3 * H, t0 - 2 * H
        self.backlog = every(a + M, b - M)
        self.cloud.add(ID_B, every(a - H, a) + every(b, t0) + self.backlog)
        self.d._register_hole(ID_B, a, b, self.sensors, now=t0)
        self.hole = self.d._recheck_holes[0]
        self.clock.t = t0 + H

    def rechecks(self):
        self.d._recheck_gaps(self.sensors, 'SensorPush')

    def has_backlog(self, w):
        return set(self.backlog) <= set(written_times(w))

    def test_rechecks_wait_while_a_backend_fails_its_writes(self):
        # B failed the cycle's poll writes (say, an unreachable InfluxDB 3):
        # no re-read call - its timeouts would also stretch the cycle - and
        # the gap waits until both backends take writes again.
        for down in ({'_consecutive_failures': 1}, {'_connected': False}):
            self.wb.__dict__.update(down)
            self.rechecks()
            self.clock.advance(5 * M)
        self.assertEqual(self.cloud.sample_calls(), [])
        self.assertEqual(self.hole['done'], 0)
        self.assertEqual(self.logged(logging.WARNING, 'gap re-read(s) wait'), [
            '1 gap re-read(s) wait: %s failed its last write; the gaps are kept and '
            're-read after a write works' % type(self.wb).__name__])
        self.wb._connected, self.wb._consecutive_failures = True, 0
        self.rechecks()
        self.assertEqual(len(self.cloud.sample_calls()), 1)
        self.assertTrue(self.has_backlog(self.wa) and self.has_backlog(self.wb))
        self.assertEqual(len(self.logged(logging.INFO, 'gap re-reads resume')), 1)
        self.assertEqual(self.d._recheck_holes, [])          # complete

    def test_a_backend_failing_while_no_gap_is_due_is_not_logged(self):
        # Every outage of one of two backends logged a 'wait' WARNING and a
        # 'resume' INFO, gaps or not (compat-r3).  Now only a re-read that
        # is due waits, and the WARNING says how many do.
        self.clock.t = self.hole['closed_at'] + 30 * M        # not due yet
        self.wb._consecutive_failures = 1
        self.rechecks()
        self.wb._consecutive_failures = 0
        self.rechecks()
        self.assertEqual(self.logged(logging.WARNING), [])
        self.assertEqual(self.logged(logging.INFO, 'resume'), [])
        self.clock.t = self.hole['closed_at'] + H              # due
        self.wb._consecutive_failures = 1
        self.rechecks()
        self.assertEqual(self.logged(logging.WARNING), [
            '1 gap re-read(s) wait: %s failed its last write; the gaps are kept and '
            're-read after a write works' % type(self.wb).__name__])
        self.assertEqual(self.cloud.sample_calls(), [])
        self.wb._consecutive_failures = 0
        self.rechecks()
        self.assertEqual(len(self.cloud.sample_calls()), 1)
        self.assertEqual(len(self.logged(logging.INFO, 'gap re-reads resume')), 1)

    def test_the_wait_says_what_each_backend_did(self):
        # docs-r5.  The WARNING said re-reads wait 'until every backend takes
        # writes again', also after a single write that a backend refused
        # (a 422 for points beyond a bucket's retention) while it took every
        # other one.  It now names, per backend, what was seen; so does the
        # DEBUG line while only reads of what the polls may have missed wait.
        self.wa._consecutive_failures = 1
        self.wb._connected = False
        self.rechecks()
        self.assertEqual(self.logged(logging.WARNING), [
            '1 gap re-read(s) wait: %s failed its last write, %s is not connected; the '
            'gaps are kept and re-read after a write works'
            % (type(self.wa).__name__, type(self.wb).__name__)])
        self.wa._consecutive_failures, self.wb._connected = 0, True
        self.rechecks()
        self.assertEqual(self.logged(logging.INFO, 'resume'),
                         ['Every backend takes writes again; gap re-reads resume'])
        self.d._register_hole(ID_B, self.hole['start'], self.hole['end'], self.sensors,
                              now=self.clock.t - H, probe=20 * M)
        self.wb._consecutive_failures = 1
        self.rechecks()
        self.assertEqual(self.logged(logging.DEBUG, 'Re-reads wait'), [
            'Re-reads wait: %s failed its last write' % type(self.wb).__name__])
        self.assertEqual(len(self.logged(logging.WARNING)), 1)

    def test_reads_of_what_the_polls_missed_wait_without_a_warning(self):
        # With polls 40-59 min apart a probe is due most cycles, so every
        # outage of one backend logged '1 gap re-read(s) wait' at WARNING
        # and the return at INFO, with no gap at all (compat-r2-1).  Only
        # gaps count; a gap that falls due while probes wait is announced.
        self.d._drop_hole(self.hole)
        t0 = self.hole['closed_at']
        self.d._register_hole(ID_B, self.hole['start'], self.hole['end'], self.sensors,
                              now=t0, probe=20 * M)
        self.wb._consecutive_failures = 1
        self.rechecks()
        self.wb._consecutive_failures = 0
        self.rechecks()
        self.assertEqual(len(self.cloud.sample_calls()), 1)
        self.assertEqual(self.logged(logging.WARNING), [])
        self.assertEqual(self.logged(logging.INFO, 'resume'), [])
        self.d._register_hole(ID_B, t0 - 6 * H, t0 - 5 * H, self.sensors, now=t0, probe=20 * M)
        self.wb._consecutive_failures = 1
        self.rechecks()
        self.d._register_hole(ID_B, t0 - 9 * H, t0 - 8 * H, self.sensors, now=t0)
        self.rechecks()
        self.rechecks()
        self.assertEqual(self.logged(logging.WARNING), [
            '1 gap re-read(s) wait: %s failed its last write; the gaps are kept and '
            're-read after a write works' % type(self.wb).__name__])
        self.wb._consecutive_failures = 0
        self.rechecks()
        self.assertEqual(len(self.logged(logging.INFO, 'gap re-reads resume')), 1)
        # One call serves both; only the gap is counted as one.
        self.assertEqual(len(self.cloud.sample_calls()), 2)
        self.assertEqual(len(self.logged(logging.INFO, 'Re-reading 1 gap(s) of 1 sensor(s)')), 1)

    def test_a_write_one_backend_missed_is_read_again(self):
        # B refuses writes only during the 1-h re-read (corr-4): the call
        # counts as failed, nothing advances, and the next one - once B
        # takes writes again - gives B the samples as well.
        real = self.wb.write
        self.wb.write = mock.Mock(side_effect=OSError('connection refused'))
        self.rechecks()
        self.assertEqual((self.hole['done'], self.hole['counts']), (0, [0]))
        self.assertTrue(self.has_backlog(self.wa))
        self.assertEqual(self.d._recheck_fail_streak, 1)
        self.wb.write = real
        self.wb._consecutive_failures = 0        # the next poll landed on B
        self.clock.advance(5 * M)
        self.rechecks()
        self.assertTrue(self.has_backlog(self.wb))
        self.assertEqual(self.d._recheck_holes, [])

    def test_dry_run_never_waits(self):
        d = self.make_daemon([self.wa, self.wb], api=self.cloud,
                             args=make_args(dryrun=True))
        d._register_hole(ID_B, self.hole['start'], self.hole['end'],
                         self.sensors, now=self.hole['closed_at'])
        self.wb._connected = False
        d._recheck_gaps(self.sensors, 'SensorPush')
        self.assertEqual(len(self.cloud.sample_calls()), 1)
        self.assertEqual(self.wa.written + self.wb.written, [])


# -----------------------------------------------------------------------------
# State
# -----------------------------------------------------------------------------
class State(RecheckCase):

    def test_round_trip(self):
        cloud = Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT.w'})
        d = self.daemon(cloud)
        t0 = self.clock.t
        d._register_hole(ID_A, t0 - 10 * D, t0 - H, cloud.sensors(), now=t0)
        d._register_hole(ID_B, t0 - 3 * H, t0 - 2 * H, cloud.sensors(), now=t0 + 1)
        long_hole = d._recheck_holes[0]
        long_hole.update(done=1, pos=3)
        long_hole['counts'][:3] = [600, 0, 17]
        long_hole['_found'] = 617              # in memory only
        d._recheck_last_seen = {ID_A: t0 - 60.5, ID_B: t0}
        d._save_state()
        state = json.loads((self.state_dir / 'state.json').read_text())
        self.assertEqual(list(state), ['sensors', 'failed_windows',
                                       'collected_until', 'rechecks'])
        self.assertEqual([list(h) for h in state['rechecks']['holes']],
                         [list(spd.HOLE_KEYS)] * 2)
        again = self.daemon(cloud)
        self.assertEqual(again._recheck_last_seen, d._recheck_last_seen)
        # The persisted keys; a hole restored in the middle of a pass also
        # gets an in-memory '_since' (see LastPass.test_the_pi_off_...).
        self.assertEqual([{k: h[k] for k in spd.HOLE_KEYS} for h in again._recheck_holes],
                         [{k: h[k] for k in spd.HOLE_KEYS} for h in d._recheck_holes])
        self.assertTrue(self.logged(logging.INFO, 'Restored 2 gap(s) to re-check'))

    def test_state_files_of_earlier_releases_load(self):
        cloud = Cloud(self.clock, {ID_A: 'HT1'})
        until = at(self.clock.t - H)
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'sensors': {}, 'failed_windows': [], 'collected_until': until.isoformat()}))
        d = self.daemon(cloud)
        self.assertEqual((d._recheck_holes, d._recheck_last_seen), ([], {}))
        self.assertEqual(d._collected_until, until)
        self.assertEqual(self.logged(logging.WARNING), [])
        d._save_state()
        self.assertEqual(json.loads((self.state_dir / 'state.json').read_text())
                         ['rechecks'], {'last_seen': {}, 'holes': []})
        # The format from before collected_until still adopts its marker.
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'sensors': {}, 'failed_windows': []}))
        (self.state_dir / 'last-success').write_text(until.isoformat() + '\n')
        self.assertEqual(self.daemon(cloud)._collected_until, until)

    def test_a_lowered_timestep_applies_to_restored_holes(self):
        t0 = self.clock.t
        self.write_state([{'sid': ID_A, 'start': t0 - 30 * H, 'end': t0 - 10 * H,
                           'closed_at': t0, 'done': 1, 'pos': 1, 'cell': 660,
                           'counts': [3, 4]}])
        d = self.daemon(Cloud(self.clock, {ID_A: 'HT1'}), args=make_args(timestep=360))
        hole = d._recheck_holes[0]
        self.assertEqual((hole['cell'], hole['pos'], hole['counts']),
                         (300, 0, [0, 0, 0, 0, 0]))

    def test_a_long_hole_survives_restarts_after_its_cells_shrank(self):
        # An HT.w away 50 days: its hole is clamped to the 45 days on board
        # by whole 11-h cells, so it spans 46 days and 8 hours.  Its cells
        # shrink later - -t lowered, or a truncated answer halved them - and
        # the hole must still load: the bound is on-board storage plus a
        # day, whatever the cell (compat-r1, corr-r1).  With cells over a
        # day long (-t 2880) the clamp keeps at most that day.
        for how, registered, restarts in (('-t lowered', 720, (360, 360)),
                                          ('truncated', 720, (720, 720)),
                                          ('-t 2880, then 720', 2880, (720, 720))):
            with self.subTest(how):
                self.reset_state()
                del self.records[:]
                t0 = self.clock.t
                start = t0 - (47.5 * D if registered == 2880 else 50 * D)
                cloud = LimitCloud(self.clock, {ID_C: 'HT.w'})
                d = self.daemon(cloud, args=make_args(timestep=registered, qlimit=500))
                d._register_hole(ID_C, start, t0, cloud.sensors(), now=t0)
                hole = d._recheck_holes[0]
                self.assertTrue(46 * D < hole['end'] - hole['start'] <= 47 * D)
                cloud.add(ID_C, every(hole['start'] - H, hole['start'] + 12 * H))
                if how == 'truncated':
                    self.clock.advance(H)
                    d._recheck_gaps(cloud.sensors(), 'SensorPush')
                    self.assertEqual((cloud.truncated_calls, hole['cell']), (1, 330))
                d._save_state()
                for timestep in restarts:
                    d = self.daemon(cloud, args=make_args(timestep=timestep))
                    self.assertEqual([(h['start'], h['end']) for h in d._recheck_holes],
                                     [(hole['start'], hole['end'])])
                    d._save_state()
                self.assertEqual(self.logged(logging.WARNING, 'malformed'), [])

    def run_cycles(self, cycles, **daemon_cfg):
        """2.5 h of 5-min cycles while sensor B is silent for 40 minutes."""
        self.reset_state()
        cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT.w'})
        cloud.add(ID_A, every(T0 - 2 * H, T0 + 3 * H))
        cloud.add(ID_B, every(T0 - 2 * H, T0 + 20 * M) + every(T0 + H, T0 + 3 * H))
        d = self.daemon(cloud, **daemon_cfg)
        self.tag_rechecks(d, cloud)
        windows = []
        compute = d._compute_daemon_window

        def spy(*a, **k):
            result = compute(*a, **k)
            windows.append(result[:2])
            return result
        d._compute_daemon_window = spy
        per_cycle = []
        for _ in range(cycles):
            before = len(cloud.calls)
            d._collect_cycle()
            per_cycle.append(cloud.calls[before:])
            self.clock.advance(5 * M)
        state = json.loads((self.state_dir / 'state.json').read_text())
        return d, per_cycle, windows, state

    def test_off_makes_exactly_the_old_calls_and_state(self):
        off, off_calls, windows, off_state = self.run_cycles(30, recheck_gaps='0')
        for calls, (start, stop) in zip(off_calls, windows):
            # Per cycle: the sensors, the reports and the main windows - no more.
            self.assertEqual(calls, [('get_sensors', None), ('get_reports', None)] + [
                ('get_samples', None, w0, w1, (), 10000)
                for w0, w1 in spd.build_timelist(start, stop, 720)])
        self.assertEqual(len(off_calls), 30)
        self.assertEqual(list(off_state), ['sensors', 'failed_windows', 'collected_until'])
        self.assertEqual((off_state['sensors'], off_state['failed_windows']), ({}, []))
        self.assertEqual((off._recheck_holes, off._recheck_last_seen), ([], {}))
        # On (the default), the same calls come first, and re-reads are added.
        on, on_calls, _, on_state = self.run_cycles(30)
        self.assertEqual([[c for c in calls if c[1] is None] for calls in on_calls],
                         off_calls)
        rechecks = [c for calls in on_calls for c in calls if c[1] == 'recheck']
        self.assertEqual(len(rechecks), 1)
        self.assertEqual(rechecks[0][4], (ID_B,))
        self.assertEqual([(h['sid'], h['done']) for h in on._recheck_holes], [(ID_B, 1)])
        self.assertEqual(list(on_state), ['sensors', 'failed_windows',
                                          'collected_until', 'rechecks'])
        self.assertEqual({k: on_state[k] for k in list(off_state)}, off_state)

    def test_dry_run_writes_neither_database_nor_state(self):
        t0 = self.clock.t
        a, b = t0 - 4 * H, t0 - 3 * H
        self.write_state([{'sid': ID_A, 'start': a, 'end': b, 'closed_at': t0 - 2 * H,
                           'done': 0, 'pos': 0, 'cell': 660, 'counts': [0]}])
        before = (self.state_dir / 'state.json').read_text()
        cloud = Cloud(self.clock, {ID_A: 'HT1'})
        cloud.add(ID_A, every(a - H, t0, 5 * M))
        w = fake_writer()
        d = self.daemon(cloud, writer=w, args=make_args(dryrun=True))
        self.tag_rechecks(d, cloud)
        d._collect_cycle()
        self.assertEqual(len(cloud.sample_calls('recheck')), 1)
        self.assertEqual(d._recheck_holes[0]['done'], 1)
        self.assertEqual(w.written, [])
        d._save_state()
        self.assertEqual((self.state_dir / 'state.json').read_text(), before)
        self.assertFalse((self.state_dir / 'last-success').exists())
        self.assertTrue(self.logged(logging.INFO, 'not written (dry run)'))


# -----------------------------------------------------------------------------
# Relation to the 6-h path
# -----------------------------------------------------------------------------
class SixHourPath(RecheckCase):

    def run_return(self, **daemon_cfg):
        """Sensor B is away for 8 h and back; three deep scans an hour apart."""
        self.reset_state()
        cloud = Cloud(self.clock, {ID_A: 'HT.w', ID_B: 'HT.w'})
        cloud.add(ID_A, every(T0 - 9 * H, T0 + 3 * H))
        cloud.add(ID_B, every(T0 - 9 * H, T0 - 8 * H) + every(T0 - 10 * M, T0 + 3 * H))
        w = fake_writer(newest={TAG_A: at(T0 - 2 * M), TAG_B: at(T0 - 8 * H)})
        d = self.daemon(cloud, writer=w, **daemon_cfg)
        self.tag_rechecks(d, cloud)
        steps = []
        for _ in range(3):
            d._collect_cycle()
            steps.append(copy.deepcopy(d._sensor_state))
            # What the database now holds.
            for r in [r for batch in w.written for r in batch
                      if r['measurement'] == 'SensorPush']:
                t = at(spd._observed_epoch(r['time']))
                tag = r['tags']['sensor_id']
                w.newest[tag] = max(w.newest.get(tag, t), t)
            del w.written[:]
            self.clock.advance(H)
        return d, cloud, steps

    def test_long_gap_is_back_filled_as_before_and_re_read_as_well(self):
        off, off_cloud, off_steps = self.run_return(recheck_gaps='0')
        on, on_cloud, on_steps = self.run_return()
        # The 6-h path does exactly what it did: record, wait, back-fill.
        self.assertEqual(on_steps, off_steps)
        self.assertEqual([set(s) for s in off_steps], [{ID_B}, {ID_B}, set()])
        self.assertEqual(on_cloud.sample_calls(), off_cloud.sample_calls())
        self.assertTrue([c for c in off_cloud.sample_calls() if c[4] == (ID_B,)])
        # ... and the detector re-reads the same gap on its own schedule.
        gaps = [h for h in on._recheck_holes if not h['probe']]
        self.assertEqual([(h['sid'], h['start'], h['end'], h['done'])
                          for h in gaps],
                         [(ID_B, T0 - 8 * H, T0 - 10 * M, 1)])
        calls = on_cloud.sample_calls('recheck')
        self.assertEqual(len([c for c in calls
                              if spd._observed_epoch(c[2]) < T0 - 8 * H]), 1)
        # Cycles an hour apart start each window just before the newest
        # sample seen, where the API's chunk selection can leave samples
        # out: each such stretch is read once more, an hour later.
        self.assertEqual(sorted(h['sid'] for h in on._recheck_holes if h['probe']),
                         [ID_A, ID_B])
        self.assertEqual(len(calls), 2)
        self.assertEqual(off._recheck_holes, [])


if __name__ == '__main__':
    unittest.main()
