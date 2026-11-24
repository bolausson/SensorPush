"""Cut-offs for sensor error values ([CUTOFFS]).

A failed temperature/humidity chip keeps reporting fixed error values: an
HT1 -46.84 C (the chip's raw 0) and 0 %, an HTP.xw -40 C and 0 % with a valid
pressure.  Config, what is stored of a record, every write path, and what
gap detection and the deep scan see.
"""
import copy
import logging
import tempfile
import unittest
from pathlib import Path

from helpers import ID_A, ID_B, ID_C, fake_writer, make_args, make_config, sensorpushd
import test_rechecks as tr

spd = sensorpushd()
M, H, D, T0 = tr.M, tr.H, tr.D, tr.T0
TAG_A, TAG_B = spd.sensor_id_tag(ID_A), spd.sensor_id_tag(ID_B)
RECOMMENDED = {'TEMPERATURE_FLOOR': '-40', 'HUMIDITY_FLOOR': '0'}

# As the API serves them (F, inHg, kPa, ft); it computes dewpoint and vpd too.
HT1_ERROR = {'temperature': -52.312, 'humidity': 0.0, 'dewpoint': 32.0, 'vpd': 0.01}
HTP_ERROR = {'temperature': -40.0, 'humidity': 0.0, 'dewpoint': 32.0, 'vpd': 0.02,
             'barometric_pressure': 29.92, 'altitude': 120.0}
HUMIDITY_ERROR = {'temperature': 70.0, 'humidity': 0.0, 'dewpoint': 32.0, 'vpd': 2.5}
GOOD = {'temperature': 70.0, 'humidity': 45.0, 'dewpoint': 48.0, 'vpd': 1.4}


def load(text):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'test.conf'
        path.write_text(text)
        return spd.load_config(str(path))


def with_cutoffs(cutoffs, **daemon):
    """make_config(), with a [CUTOFFS] section as the loaders return it."""
    return dict(make_config(**daemon), cutoffs=dict(cutoffs))


class ErrorCloud(tr.Cloud):
    """A Cloud whose samples listed in ``errors`` (sid -> times) carry a
    failed chip's values: -40 F (also -40 C) and 0 %, with the dewpoint and
    vpd the API computes from them.  HTP.xw samples carry a pressure."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.errors = {}

    def answer(self, start, stop, sensors=None, limit=10000):
        out = super().answer(start, stop, sensors, limit)
        for sid, items in out['sensors'].items():
            bad = self.errors.get(sid, ())
            for item in items:
                if spd._observed_epoch(item['observed']) in bad:
                    item.update(temperature=-40.0, humidity=0.0, dewpoint=32.0,
                                vpd=0.02)
                if self.types[sid] == 'HTP.xw':
                    item['barometric_pressure'] = 29.92
        return out


class Config(tr.RecheckCase):

    def test_no_section_or_an_empty_one_loads_as_before(self):
        for text in (tr.UNIFIED, tr.UNIFIED + '[CUTOFFS]\n# TEMPERATURE_FLOOR = -40\n',
                     tr.LEGACY_IFDB2, tr.LEGACY_VM + '[CUTOFFS]\n'):
            cfg = load(text)
            self.assertNotIn('cutoffs', cfg)
            d = spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()], cfg,
                                     make_args())
            self.assertEqual(d._cutoffs, {})
        self.assertEqual(self.logged(logging.INFO, 'Cut-offs'), [])

    def test_the_template_has_the_section_commented_out(self):
        for line in ('[CUTOFFS]', '# TEMPERATURE_FLOOR = -40', '# HUMIDITY_FLOOR = 0'):
            self.assertIn(line + '\n', spd.DEFAULT_CONFIG_TEMPLATE)
        other = [l for l in spd.DEFAULT_CONFIG_TEMPLATE.splitlines()
                 if l.startswith('# Also <METRIC>_CEILING')]
        self.assertEqual(len(other), 1)
        for field in spd.CUTOFF_FIELDS[2:]:
            self.assertIn(field.upper(), other[0])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'sensorpushd.conf'
            spd.create_default_config(str(path))
            self.assertNotIn('cutoffs', spd.load_config(str(path)))

    def test_every_loader_reads_the_section(self):
        for base in (tr.UNIFIED, tr.LEGACY_IFDB2, tr.LEGACY_VM):
            with self.subTest(unified=base is tr.UNIFIED):
                cfg = load(base + '[CUTOFFS]\ntemperature_floor = -40\n'
                           'HUMIDITY_FLOOR =\nPRESSURE_CEILING = 1100.5\n')
                self.assertEqual(cfg['cutoffs'], {'TEMPERATURE_FLOOR': '-40',
                                                  'HUMIDITY_FLOOR': '',
                                                  'PRESSURE_CEILING': '1100.5'})
                d = spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()], cfg,
                                         make_args(daemon=False))
                self.assertEqual(d._cutoffs, {'temperature': (-40.0, None),
                                              'pressure': (None, 1100.5)})

    def test_values(self):
        self.assertEqual(spd.parse_cutoffs({}), {})
        self.assertEqual(spd.parse_cutoffs({
            'TEMPERATURE_FLOOR': ' -40 ', 'TEMPERATURE_CEILING': '60', 'HUMIDITY_FLOOR': '0',
            'RSSI_FLOOR': '-120.5', 'VPD_CEILING': '  ', 'ALTITUDE_FLOOR': '1e-3'}),
            {'temperature': (-40.0, 60.0), 'humidity': (0.0, None),
             'rssi': (-120.5, None), 'altitude': (0.001, None)})

    def test_invalid_values_exit_at_startup(self):
        cloud = tr.Cloud(self.clock, {ID_A: 'HT1'})
        for raw, named in (
                ({'TEMPERATURE_FLOOR': 'abc'}, ["TEMPERATURE_FLOOR = 'abc' is not a number"]),
                ({'TEMPERATURE_FLOOR': 'nan'}, ['TEMPERATURE_FLOOR']),
                ({'HUMIDITY_FLOOR': '-inf'}, ['HUMIDITY_FLOOR']),
                ({'HUMIDITY_FLOOR': '0  # the error value'}, ['HUMIDITY_FLOOR']),
                ({'HUMIDITY_FLOOR': '0,5'}, ['HUMIDITY_FLOOR']),
                ({'TEMPRATURE_FLOOR': '-40', 'HUMIDITY_FLOR': '0'},
                 ['unknown keys TEMPRATURE_FLOOR, HUMIDITY_FLOR']),
                ({'HUMIDITY_FLOOR': '50', 'HUMIDITY_CEILING': '50'},
                 ['HUMIDITY_FLOOR = 50 is not below HUMIDITY_CEILING = 50']),
                ({'TEMPERATURE_FLOOR': '10', 'TEMPERATURE_CEILING': '5', 'VPD_FLOOR': 'x'},
                 ['TEMPERATURE_FLOOR = 10 is not below TEMPERATURE_CEILING = 5',
                  "VPD_FLOOR = 'x' is not a number"])):
            # Cut-offs apply in every mode, so they are checked in every mode.
            for daemon in (True, False):
                with self.subTest(raw=raw, daemon=daemon):
                    del self.records[:]
                    with self.assertRaises(SystemExit) as cm:
                        spd.SensorPushDaemon(cloud, [fake_writer()], with_cutoffs(raw),
                                             make_args(daemon=daemon))
                    self.assertEqual(cm.exception.code, 2)
                    errors = self.logged(logging.ERROR)
                    self.assertEqual(len(errors), 1)
                    self.assertTrue(errors[0].startswith('Invalid [CUTOFFS] setting: '))
                    for text in named:
                        self.assertIn(text, errors[0])
                    self.assertIn('The valid keys are <METRIC>_FLOOR and <METRIC>_CEILING '
                                  'for TEMPERATURE, HUMIDITY, PRESSURE, DISTANCE, DEWPOINT, '
                                  'VPD, ABS_HUMIDITY, ALTITUDE, VOLTAGE, RSSI', errors[0])
                    self.assertNotIn('\n', errors[0])
        self.assertEqual(cloud.calls, [])
        self.assertFalse((self.state_dir / 'state.json').exists())

    def test_a_misnamed_section_exits_at_startup(self):
        # Section names are case-sensitive: [cutoffs], [CUTOFF] or [Cut-Offs]
        # is not read, and the error values someone meant to cut off would
        # be stored without a word.  An empty one is harmless.
        for base in (tr.UNIFIED, tr.LEGACY_VM):
            for name in ('cutoffs', 'CUTOFF', 'Cut-Offs', 'cut_off'):
                with self.subTest(name=name, unified=base is tr.UNIFIED):
                    del self.records[:]
                    cfg = load(base + '[%s]\nTEMPERATURE_FLOOR = -40\n' % name)
                    self.assertNotIn('cutoffs', cfg)
                    with self.assertRaises(SystemExit) as cm:
                        spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()], cfg,
                                             make_args(daemon=False))
                    self.assertEqual(cm.exception.code, 2)
                    self.assertEqual(self.logged(logging.ERROR), [
                        'Invalid [CUTOFFS] setting: [%s] is not read: the section must be '
                        'called [CUTOFFS] (section names are case-sensitive)' % name])
        cfg = load(tr.UNIFIED + '[cutoffs]\n# TEMPERATURE_FLOOR = -40\n')
        self.assertNotIn('cutoffs_misnamed', cfg)
        d = spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()], cfg, make_args())
        self.assertEqual(d._cutoffs, {})

    def test_a_percent_sign_reaches_the_parser(self):
        # Read raw, as the RECHECK keys: an interpolation error would stop
        # loading the file in every mode, --listsensors too.
        cfg = load(tr.UNIFIED + '[CUTOFFS]\nHUMIDITY_FLOOR = 0%\n')
        self.assertEqual(cfg['cutoffs'], {'HUMIDITY_FLOOR': '0%'})
        with self.assertRaises(SystemExit):
            spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()], cfg, make_args())
        self.assertIn('HUMIDITY_FLOOR', self.logged(logging.ERROR)[0])

    def test_startup_line(self):
        spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()],
                             with_cutoffs(dict(RECOMMENDED, RSSI_CEILING='0')), make_args())
        self.assertEqual(self.logged(logging.INFO, 'Cut-offs: '), [
            'Cut-offs: TEMPERATURE_FLOOR = -40, HUMIDITY_FLOOR = 0, RSSI_CEILING = 0. A value '
            'at or below its floor, or at or above its ceiling, is not stored, nor are the '
            'values computed from it'])


class WhatIsStored(tr.RecheckCase):
    """_cut_records() on the records process_samples() builds."""

    def daemon_with(self, cutoffs, **args):
        return spd.SensorPushDaemon(tr.Cloud(self.clock, {}), [fake_writer()],
                                    with_cutoffs(cutoffs), make_args(**args))

    def build(self, *samples, noconvert=False):
        """One record per API item of sensor A (named S0), a minute apart."""
        items = [dict(s, observed=tr.iso(T0 + k * M)) for k, s in enumerate(samples)]
        return spd.process_samples({'sensors': {ID_A: items}}, tr.Cloud(
            self.clock, {ID_A: 'HTP.xw'}).sensors(), 'SensorPush', 0.0, noconvert)

    def fields(self, d, records):
        return [r['fields'] for r in d._cut_records(records, {})]

    def test_the_recommendation_cuts_each_error_point_as_it_should(self):
        d = self.daemon_with(RECOMMENDED)
        out = self.fields(d, self.build(HT1_ERROR, HTP_ERROR, HUMIDITY_ERROR, GOOD))
        # The HT1 error point is not stored at all; the HTP.xw one keeps its
        # pressure and the altitude; the humidity-only error its temperature
        # (and the altitude).
        self.assertEqual(out, [{'pressure': 1013.21, 'altitude': 36.58},
                               {'temperature': 21.11, 'altitude': 0.0},
                               self.build(GOOD)[0]['fields']])

    def test_inclusive_at_both_ends(self):
        d = self.daemon_with({'TEMPERATURE_FLOOR': '-40', 'TEMPERATURE_CEILING': '50'})
        # -40 C and 50 C (-40 F and 122 F) are cut, just inside them kept.
        temps = [-40.0, -39.982, 121.982, 122.0, -52.312]
        out = self.fields(d, self.build(*[dict(GOOD, temperature=t) for t in temps]))
        self.assertEqual([f.get('temperature') for f in out], [None, -39.99, 49.99, None, None])
        # So a ceiling of 100 % would cut real readings of 100 %.
        d = self.daemon_with({'HUMIDITY_CEILING': '100'})
        out = self.fields(d, self.build(dict(GOOD, humidity=100.0), dict(GOOD, humidity=99.9)))
        self.assertEqual([f.get('humidity') for f in out], [None, 99.9])

    def test_per_value_and_the_values_computed_from_it(self):
        # Pressure: abs_humidity and altitude go with it; dewpoint and vpd
        # are cut on their own; temperature and humidity stay.
        d = self.daemon_with({'PRESSURE_FLOOR': '500', 'DEWPOINT_CEILING': '30',
                              'VPD_FLOOR': '0.5'})
        rec = self.build(dict(HTP_ERROR, temperature=70.0, humidity=45.0,
                                barometric_pressure=10.0, dewpoint=100.0, vpd=0.04))
        self.assertEqual(self.fields(d, rec), [{'humidity': 45.0, 'temperature': 21.11}])
        # Humidity: dewpoint, vpd and abs_humidity go with it.
        d = self.daemon_with({'HUMIDITY_FLOOR': '0'})
        self.assertEqual(self.fields(d, self.build(HUMIDITY_ERROR)),
                         [{'temperature': 21.11, 'altitude': 0.0}])

    def test_a_temperature_error_alone_and_a_distance(self):
        # A temperature cut off alone (a sauna sensor at its ceiling, say)
        # takes dewpoint, vpd and abs_humidity with it, and leaves the
        # humidity and the pressure.
        d = self.daemon_with({'TEMPERATURE_CEILING': '50'})
        hot = dict(GOOD, temperature=140.0)          # 60 C
        out = self.fields(d, self.build(hot, dict(hot, barometric_pressure=29.92)))
        self.assertEqual(out, [{'humidity': 45.0, 'altitude': 0.0},
                               {'humidity': 45.0, 'pressure': 1013.21, 'altitude': 0.0}])
        # A distance is a measured value: an error point that has one keeps
        # it (and the altitude).
        d = self.daemon_with(RECOMMENDED)
        out = self.fields(d, self.build(dict(HT1_ERROR, distance=20.0)))
        self.assertEqual(out, [{'altitude': 0.0, 'distance': 6.1}])

    def test_a_sample_left_without_a_measured_value_is_not_stored(self):
        d = self.daemon_with({'TEMPERATURE_FLOOR': '-40', 'HUMIDITY_FLOOR': '0',
                              'PRESSURE_CEILING': '1000'})
        cut = {}
        self.assertEqual(d._cut_records(self.build(HTP_ERROR, HT1_ERROR), cut), [])
        # cut[None]: the newest sample whose temperature was cut off.
        self.assertEqual(cut, {('S0', 'temperature'): 2, ('S0', 'humidity'): 2,
                               ('S0', 'pressure'): 1, ('S0', None): 2, None: T0 + M})

    def test_units_with_noconvert(self):
        # -n stores what the API serves: F, inHg, kPa, ft.  -40 is the same
        # in F and C, an HT1's raw 0 is -52.31 F, and a freezer at -21.32 C
        # (-6.38 F) is no error value either way.
        d = self.daemon_with(RECOMMENDED, noconvert=True)
        freezer = dict(GOOD, temperature=-6.376)
        out = self.fields(d, self.build(HTP_ERROR, HT1_ERROR, freezer, noconvert=True))
        self.assertEqual([f.get('temperature') for f in out], [None, -6.376])
        self.assertEqual(out[0], {'pressure': 29.92, 'altitude': 120.0})
        # DISTANCE_CEILING = 10: 20 ft is stored as 6.1 m, but as 20 with -n.
        far = dict(GOOD, distance=20.0)
        d = self.daemon_with({'DISTANCE_CEILING': '10'})
        self.assertEqual(self.fields(d, self.build(far))[0]['distance'], 6.1)
        d = self.daemon_with({'DISTANCE_CEILING': '10'}, noconvert=True)
        self.assertNotIn('distance', self.fields(d, self.build(far, noconvert=True))[0])

    def test_a_record_without_the_field_is_not_affected(self):
        # A sample without pressure or distance, status records without
        # temperature, samples without voltage.
        d = self.daemon_with({'PRESSURE_FLOOR': '2000', 'DISTANCE_FLOOR': '5',
                              'VOLTAGE_FLOOR': '5'})
        samples = self.build(GOOD)
        self.assertIs(d._cut_records(samples, {}), samples)
        status = spd.build_voltage_records(tr.Cloud(self.clock, {ID_A: 'HT1'}).sensors(),
                                           'SensorPush', tr.at(T0))
        self.assertEqual(self.fields(d, status), [{'rssi': -60.0}])
        d = self.daemon_with({'TEMPERATURE_FLOOR': '100'})
        self.assertIs(d._cut_records(status, {}), status)

    def test_status_records(self):
        sensors = {ID_A: {'id': ID_A, 'name': 'S0', 'rssi': -70},          # no voltage
                   ID_B: {'id': ID_B, 'name': 'S1', 'battery_voltage': 3.1, 'rssi': -70},
                   ID_C: {'id': ID_C, 'name': 'S2'}}                       # neither
        status = spd.build_voltage_records(sensors, 'SensorPush', tr.at(T0))
        self.assertEqual([r['fields'] for r in status], [
            {'voltage': 0.0, 'rssi': -70.0}, {'voltage': 3.1, 'rssi': -70.0},
            {'voltage': 0.0, 'rssi': 0.0}])
        # The 0.0 of a voltage the API did not report is an ordinary value:
        # VOLTAGE_FLOOR = 0 cuts it.  A status record left with neither
        # field is not stored.
        d = self.daemon_with({'VOLTAGE_FLOOR': '0', 'RSSI_CEILING': '0'})
        self.assertEqual(self.fields(d, status), [{'rssi': -70.0},
                                                  {'voltage': 3.1, 'rssi': -70.0}])

    def test_cut_records_are_copies(self):
        records = self.build(GOOD, GOOD)
        self.assertIs(self.daemon_with({})._cut_records(records, {}), records)
        self.assertIs(self.daemon_with(RECOMMENDED)._cut_records(records, {}), records)
        error = self.build(HUMIDITY_ERROR)
        out = self.daemon_with(RECOMMENDED)._cut_records(error, {})
        self.assertNotIn('humidity', out[0]['fields'])
        self.assertIn('humidity', error[0]['fields'])
        self.assertIs(out[0]['tags'], error[0]['tags'])

    def test_logging_once_per_sensor_and_metric(self):
        d = self.daemon_with(RECOMMENDED)
        for _ in range(3):
            d._apply_cutoffs(self.build(HT1_ERROR, HT1_ERROR, HUMIDITY_ERROR, GOOD))
        warnings = self.logged(logging.WARNING)
        self.assertEqual(len(warnings), 2)
        when = spd._fmt_time(T0)
        self.assertIn(
            "Sensor 'S0': temperature -46.84 at %s is at or below TEMPERATURE_FLOOR = -40, "
            "an error value: it is not stored, nor are dewpoint, vpd and abs_humidity, "
            "computed from it. Further error values of its temperature are counted in INFO "
            "lines" % when, warnings)
        self.assertIn(
            "Sensor 'S0': humidity 0 at %s is at or below HUMIDITY_FLOOR = 0, an error value: "
            "it is not stored, nor are dewpoint, vpd and abs_humidity, computed from it. "
            "Further error values of its humidity are counted in INFO lines" % when, warnings)
        # One INFO line per batch, per sensor and metric.
        self.assertEqual(self.logged(logging.INFO, 'Cut-offs left out'), [
            "Cut-offs left out 5 error value(s): 'S0' temperature 2, humidity 3 "
            "(2 records not stored at all)"] * 3)
        d._apply_cutoffs(self.build(GOOD))
        self.assertEqual(len(self.logged(logging.INFO, 'Cut-offs left out')), 3)

    def test_logging_names_each_sensor_the_bound_it_hit_and_the_values_time(self):
        # Two sensors, a floor and a ceiling, values an hour older than the
        # clock: one WARNING per sensor and metric - S1's second humidity
        # error, at the floor, is only counted - each naming the key the
        # value hit and the value's time.
        d = self.daemon_with({'HUMIDITY_FLOOR': '0', 'HUMIDITY_CEILING': '100'})
        self.clock.advance(H)
        items = {ID_A: [dict(GOOD, humidity=0.0, observed=tr.iso(T0))],
                 ID_B: [dict(GOOD, humidity=100.0, observed=tr.iso(T0 + M)),
                        dict(GOOD, humidity=0.0, observed=tr.iso(T0 + 2 * M))]}
        sensors = tr.Cloud(self.clock, {ID_A: 'HT1', ID_B: 'HT1'}).sensors()
        records = spd.process_samples({'sensors': items}, sensors, 'SensorPush', 0.0, False)
        for _ in range(2):
            d._apply_cutoffs(records)
        rest = ('an error value: it is not stored, nor are dewpoint, vpd and abs_humidity, '
                'computed from it. Further error values of its humidity are counted in INFO '
                'lines')
        self.assertEqual(self.logged(logging.WARNING), [
            "Sensor 'S0': humidity 0 at %s is at or below HUMIDITY_FLOOR = 0, %s"
            % (spd._fmt_time(T0), rest),
            "Sensor 'S1': humidity 100 at %s is at or above HUMIDITY_CEILING = 100, %s"
            % (spd._fmt_time(T0 + M), rest)])
        self.assertEqual(self.logged(logging.INFO, 'Cut-offs left out'), [
            "Cut-offs left out 3 error value(s): 'S0' humidity 1; 'S1' humidity 2"] * 2)


class WritePaths(tr.RecheckCase):
    """Every path that stores records leaves the error values out."""

    def setUp(self):
        super().setUp()
        self.cloud = ErrorCloud(self.clock, {ID_A: 'HT1', ID_B: 'HTP.xw'})
        self.sensors = self.cloud.sensors()
        for sid in (ID_A, ID_B):
            self.cloud.add(sid, tr.every(T0 - 2 * D, T0 + 2 * D, 5 * M))
        # A's chip failed 10 h ago.  B's failed twice for a while: in what a
        # gap re-read reads below, and in the last hour, which the polls read.
        self.cloud.errors[ID_A] = set(tr.every(T0 - 10 * H, T0 + 2 * D, 5 * M))
        self.cloud.errors[ID_B] = set(tr.every(T0 - 4 * H, T0 - 3 * H, 5 * M)
                                      + tr.every(T0 - 50 * M, T0 - 30 * M, 5 * M))

    def make(self, args=None, cutoffs=RECOMMENDED):
        w = fake_writer(newest={TAG_A: tr.at(T0 - 10 * H), TAG_B: tr.at(T0 - 2 * M)})
        d = spd.SensorPushDaemon(self.cloud, [w], with_cutoffs(cutoffs), args or make_args())
        d._interruptible_sleep = self.sleep
        return d, w

    def stored(self, w, measurement='SensorPush'):
        """(sensor_id, time, fields) of what the writer got."""
        return [(r['tags']['sensor_id'], spd._observed_epoch(r['time']), r['fields'])
                for batch in w.written for r in batch if r['measurement'] == measurement]

    def assert_no_error_values(self, stored):
        self.assertTrue(stored)
        for _, _, fields in stored:
            self.assertGreater(fields.get('temperature', 0), -40)
            self.assertGreater(fields.get('humidity', 1), 0)

    def assert_errors_keep_the_pressure(self, stored):
        errors = [f for tag, t, f in stored if tag == TAG_B and t in self.cloud.errors[ID_B]]
        self.assertTrue(errors)
        self.assertEqual({tuple(sorted(f)) for f in errors}, {('altitude', 'pressure')})

    def test_polls_and_one_shot_runs(self):
        for daemon in (True, False):
            with self.subTest(daemon=daemon):
                self.reset_state()
                del self.records[:]
                d, w = self.make(args=make_args(daemon=daemon, backlog='1h'))
                d._collect_cycle()
                stored = self.stored(w)
                self.assert_no_error_values(stored)
                # A's error points are not stored; B's keep the pressure.
                self.assertFalse([s for s in stored if s[0] == TAG_A])
                self.assert_errors_keep_the_pressure(stored)
                self.assertEqual(len(self.logged(logging.INFO, 'Cut-offs left out')), 1)

    def test_back_fills(self):
        d, w = self.make()
        self.assertTrue(d._backfill_sensor(ID_B, tr.at(T0 - 5 * H), tr.at(T0), self.sensors,
                                           'SensorPush'))
        stored = self.stored(w)
        self.assertEqual({s[0] for s in stored}, {TAG_B})
        self.assert_no_error_values(stored)
        self.assert_errors_keep_the_pressure(stored)

    def test_gap_re_reads_and_their_counts(self):
        # B's gap: its late backlog holds error values.  The re-read writes
        # what is not an error value; its count is of the samples served.
        d, w = self.make()
        a, b = T0 - 4 * H - 20 * M, T0 - 2 * H - 40 * M
        d._register_hole(ID_B, a, b, self.sensors, now=T0)
        hole = d._recheck_holes[0]
        self.clock.advance(H)
        d._recheck_gaps(self.sensors, 'SensorPush')
        interior = [t for t in self.cloud.samples[ID_B] if a < t < b]
        self.assertEqual(hole['counts'], [len(interior)])
        stored = self.stored(w)
        self.assert_no_error_values(stored)
        self.assert_errors_keep_the_pressure(stored)
        self.assertEqual(len(self.logged(logging.INFO, 'Cut-offs left out')), 1)

    def test_status_records(self):
        d, w = self.make(cutoffs={'VOLTAGE_FLOOR': '3.0'})
        d._collect_cycle()
        status = self.stored(w, 'SensorPush_V')
        self.assertEqual([f for _, _, f in status], [{'rssi': -60.0}] * 2)
        self.assertEqual(self.logged(logging.INFO, 'Cut-offs left out'), [
            "Cut-offs left out 2 error value(s): 'S0' voltage 1; 'S1' voltage 1"])

    def test_dry_run_shows_what_would_be_stored(self):
        d, w = self.make(args=make_args(dryrun=True, verbose=True))
        d._collect_cycle()
        self.assertEqual(w.written, [])
        # A lone dict argument is kept as the log record's args.
        shown = [r.args for r in self.records if r.msg == '%s' and isinstance(r.args, dict)]
        samples = [r for r in shown if r['measurement'] == 'SensorPush']
        self.assertTrue(samples)
        self.assertFalse([r for r in samples if r['tags']['sensor_id'] == TAG_A])
        self.assertTrue([r for r in samples if set(r['fields']) == {'pressure', 'altitude'}])
        self.assertTrue(self.logged(logging.INFO, 'Cut-offs left out'))


class Detection(tr.RecheckCase):
    """Gap detection and the deep scan see the samples served, error values
    included: a sensor that sends only error values has no gap and has not
    fallen behind."""

    def test_error_values_make_no_gap(self):
        cloud = ErrorCloud(self.clock, {ID_A: 'HT.w'})
        cloud.add(ID_A, tr.every(T0 - 4 * H, T0))
        cloud.errors[ID_A] = set(tr.every(T0 - 2 * H, T0))
        w = fake_writer()
        d = spd.SensorPushDaemon(cloud, [w], with_cutoffs(RECOMMENDED), make_args())
        d._recheck_last_seen = {ID_A: T0 - 4 * H}
        window = [d._window_str(T0 - 4 * H), d._window_str(T0)]
        self.assertTrue(d._fetch_and_write_window(window, 1, 1, cloud.sensors(), 'SensorPush'))
        self.assertEqual(d._recheck_holes, [])
        self.assertEqual(d._recheck_last_seen[ID_A], T0)
        self.assertLess(max(spd._observed_epoch(r['time']) for b in w.written for r in b),
                        T0 - 2 * H)

    def run_day(self, b_last_real, track=True, hours=24):
        """A (HT1) reports normally.  B (HT1) has sent only error values
        since ``b_last_real``, its newest stored temperature.  5-min cycles
        with an hourly deep scan; the backend's newest point follows what is
        written.  ``track=False`` forgets before each cycle what was cut, as
        a build that counts only stored points would.  Returns the probe
        calls and B's back-fill calls from the second deep scan on, the
        sensor state after each deep scan, and the daemon."""
        cloud = ErrorCloud(self.clock, {ID_A: 'HT1', ID_B: 'HT1'})
        for sid in (ID_A, ID_B):
            cloud.add(sid, tr.every(T0 - 2 * D, T0 + 2 * D, 5 * M))
        cloud.errors[ID_B] = set(tr.every(b_last_real + 5 * M, T0 + 2 * D, 5 * M))
        w = fake_writer(newest={TAG_A: tr.at(T0 - 3 * M), TAG_B: tr.at(b_last_real)})
        d = spd.SensorPushDaemon(cloud, [w], with_cutoffs(RECOMMENDED, recheck_gaps='0'),
                                 make_args())
        d._interruptible_sleep = self.sleep
        scans = []
        for k in range(hours * 12):
            if not track:
                d._cut_newest.clear()
            n0 = len(cloud.calls)
            d._collect_cycle()
            for r in [r for b in w.written for r in b
                      if r['measurement'] == 'SensorPush' and 'temperature' in r['fields']]:
                t = tr.at(spd._observed_epoch(r['time']))
                tag = r['tags']['sensor_id']
                w.newest[tag] = max(w.newest.get(tag, t), t)
            del w.written[:]
            if k % 12 == 0:
                scans.append((n0, copy.deepcopy(d._sensor_state)))
            self.clock.advance(5 * M)
        later = cloud.calls[scans[1][0]:]
        probes = [c for c in later if c[0] == 'get_samples' and c[5] == 1]
        backfills = [c for c in later if c[0] == 'get_samples' and c[4] == (ID_B,)
                     and c[5] == 10000]
        return probes, backfills, scans, d

    def test_a_sensor_sending_only_error_values_costs_no_probe_or_back_fill(self):
        probes, backfills, scans, d = self.run_day(T0 - H)
        self.assertEqual((probes, backfills), ([], []))
        self.assertEqual([s for _, s in scans], [{}] * 24)
        self.assertEqual(self.logged(logging.INFO, 'has fallen behind'), [])
        # Its newest sample, cut off, counts as its last data.
        self.assertGreater(d._cut_newest[TAG_B], self.clock.t - 15 * M)
        # Counting only stored points, B falls behind after 6 h, is probed
        # and back-filled - and again and again, each time from its last
        # real reading.
        del self.records[:]
        self.reset_state()
        probes, backfills, _, _ = self.run_day(T0 - H, track=False)
        self.assertGreaterEqual(len(probes), 3)
        self.assertGreaterEqual(len(self.logged(logging.INFO, 'back-fill complete')), 3)

    def test_after_a_restart_at_most_one_back_fill(self):
        # The daemon starts when B has sent only error values for a day: the
        # first deep scan sees its newest stored temperature and records its
        # gap.  The next two probe it, as for any gap on file (it reports
        # again; it settled), the second back-fills it, and that is all.
        probes, backfills, scans, d = self.run_day(T0 - D)
        self.assertEqual(set(scans[0][1]), {ID_B})
        self.assertEqual(len(probes), 2)
        self.assertTrue(backfills)
        self.assertEqual(len(self.logged(logging.INFO, 'back-fill complete')), 1)
        self.assertEqual([s for _, s in scans[2:]], [{}] * 22)
        # None of B's error values was stored, so it was the cut-offs that
        # kept B from looking behind.
        self.assertEqual(d.writers[0].newest[TAG_B], tr.at(T0 - D))

    def run_cycles(self, cloud, writers, cycles, cutoffs=RECOMMENDED, each=None):
        """5-min cycles with an hourly deep scan, RECHECK_GAPS = 0; each
        writer's newest point follows what it got, and w.stored collects
        (sensor_id, time) of its samples with a temperature.  ``each(k)``
        runs before cycle k.  Returns the daemon and the get_samples calls
        of each cycle."""
        d = spd.SensorPushDaemon(cloud, writers, with_cutoffs(cutoffs, recheck_gaps='0'),
                                 make_args())
        d._interruptible_sleep = self.sleep
        for w in writers:
            w.stored = set()
        per_cycle = []
        for k in range(cycles):
            if each:
                each(k)
            n0 = len(cloud.calls)
            d._collect_cycle()
            for w in writers:
                for r in [r for b in w.written for r in b
                          if r['measurement'] == 'SensorPush' and 'temperature' in r['fields']]:
                    t = spd._observed_epoch(r['time'])
                    tag = r['tags']['sensor_id']
                    w.newest[tag] = max(w.newest.get(tag, tr.at(t)), tr.at(t))
                    w.stored.add((tag, t))
                del w.written[:]
            per_cycle.append([c for c in cloud.calls[n0:] if c[0] == 'get_samples'])
            self.clock.advance(5 * M)
        return d, per_cycle

    def test_a_collector_whose_sensors_all_send_error_values_does_not_look_behind(self):
        # The only sensor polled has sent only error values for an hour, and
        # does so for a day.  Nothing with a temperature is stored, so the
        # backend's newest point stays the last real reading; but it took
        # every batch, so it holds everything up to the newest sample cut
        # off.  Counted from its newest stored point, every deep scan found
        # a collector gap and fetched all of the error values again, a
        # window that grew by an hour per hour (5 calls at the 47th hour; a
        # month in error, 61 calls and an hour of --delay a deep scan).
        # Now every cycle costs one call, as without cut-offs.
        cloud = ErrorCloud(self.clock, {ID_A: 'HT1'})
        cloud.add(ID_A, tr.every(T0 - 2 * D, T0 + 2 * D, 5 * M))
        cloud.errors[ID_A] = set(tr.every(T0 - H + 5 * M, T0 + 2 * D, 5 * M))
        w = fake_writer(newest={TAG_A: tr.at(T0 - H)})
        d, per_cycle = self.run_cycles(cloud, [w], 24 * 12)
        self.assertEqual([len(c) for c in per_cycle], [1] * (24 * 12))
        self.assertEqual(self.logged(logging.INFO, 'Collector gap'), [])
        self.assertEqual(self.logged(logging.INFO, 'has fallen behind'), [])
        self.assertEqual(w.newest[TAG_A], tr.at(T0 - H))     # nothing stored
        self.assertGreater(w._cut_upto, self.clock.t - 15 * M)

    def test_a_backend_that_missed_batches_still_looks_behind(self):
        # A reports normally, B sends only error values.  The second
        # backend is down from 1 h to 4 h in (it cannot connect), and back
        # for the deep scan at 4 h.  It took none of the batches of those 3
        # hours, so none of their samples cut off counts for it: its newest
        # point is A's from before, the deep scan finds its collector gap,
        # and the fetch from there gives it A's samples of the 3 hours.
        # Crediting it with B's newest sample cut off, which the other
        # backend's batches held, hid that gap.
        cloud = ErrorCloud(self.clock, {ID_A: 'HT1', ID_B: 'HT1'})
        for sid in (ID_A, ID_B):
            cloud.add(sid, tr.every(T0 - 2 * D, T0 + 2 * D, 5 * M))
        cloud.errors[ID_B] = set(tr.every(T0 - D, T0 + 2 * D, 5 * M))
        newest = {TAG_A: tr.at(T0 - 3 * M), TAG_B: tr.at(T0 - D)}
        w1, w2 = fake_writer(newest=newest), fake_writer(newest=newest)
        down = [False]
        real_connect = w2.connect

        def connect():
            if down[0]:
                raise ConnectionError('simulated outage')
            real_connect()
        w2.connect = connect

        def each(k):
            if k == 12:                              # 1 h in, after the deep scan
                down[0] = True
                w2._connected = False
            elif k == 48:                            # back for the deep scan at 4 h
                down[0] = False
        d, per_cycle = self.run_cycles(cloud, [w1, w2], 5 * 12, each=each)
        gaps = self.logged(logging.INFO, 'Collector gap')
        self.assertEqual(len(gaps), 1)
        self.assertIn('Collector gap: %s last stored data at' % type(w2).__name__, gaps[0])
        # The polls after 4 h reach back an hour (POLL_BACKLOG): the rest
        # of the 3 hours came with the fetch from the collector gap.
        outage = {(TAG_A, t) for t in cloud.samples[ID_A] if T0 + H <= t < T0 + 4 * H}
        self.assertEqual(len(outage), 36)
        self.assertTrue(outage <= w2.stored)
        # None of B's error values was stored (its back-fill rewrote some of
        # its real readings from before T0 - 1 d).
        self.assertFalse([s for s in w2.stored | w1.stored if s[0] == TAG_B and s[1] >= T0 - D])

    def test_a_dead_sensor_whose_voltage_is_cut_still_falls_behind(self):
        # B stops reporting 1 h before the start; the voltage of its status
        # record, 3.0 V, is cut by VOLTAGE_FLOOR = 3.5 in every cycle.  Only
        # a sample whose temperature was cut is one the sensor sent: B
        # falls behind, and when it is back after 10 h with its backlog, it
        # is back-filled.  Taken as B's last data, its status records kept
        # it current, and with the re-checks off 119 of its 120 samples of
        # the 10 hours were lost.
        cloud = ErrorCloud(self.clock, {ID_A: 'HT1', ID_B: 'HT1'})
        cloud.add(ID_A, tr.every(T0 - 2 * D, T0 + 2 * D, 5 * M))
        cloud.add(ID_B, tr.every(T0 - 2 * D, T0 - H, 5 * M))
        w = fake_writer(newest={TAG_A: tr.at(T0 - 3 * M), TAG_B: tr.at(T0 - H)})

        def each(k):
            if k == 10 * 12:                         # B is back, with its backlog
                cloud.add(ID_B, tr.every(T0 - H, T0 + 2 * D, 5 * M))
        d, _ = self.run_cycles(cloud, [w], 16 * 12, cutoffs={'VOLTAGE_FLOOR': '3.5'},
                               each=each)
        self.assertEqual(len(self.logged(logging.INFO, 'has fallen behind')), 1)
        self.assertEqual(len(self.logged(logging.INFO, 'back-fill complete')), 1)
        gap = {(TAG_B, t) for t in tr.every(T0 - H + 5 * M, T0 + 9 * H, 5 * M)}
        self.assertEqual(len(gap), 120)
        self.assertTrue(gap <= w.stored)
        self.assertEqual(d._cut_newest, {})


if __name__ == '__main__':
    unittest.main()
