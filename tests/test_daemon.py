"""Daemon behaviour: gap window, failed lookups, dry-run, SENSOR_UID."""
import datetime
import json
import time
import unittest

from helpers import (DaemonTestCase, FakeAPI, ID_A, ID_B, ID_C, UTC,
                     fake_writer, make_args, sensorpushd, sensors)

spd = sensorpushd()
TAG_A, TAG_B = spd.sensor_id_tag(ID_A), spd.sensor_id_tag(ID_B)
NOW = datetime.datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
H = datetime.timedelta(hours=1)
BACKLOG = datetime.timedelta(minutes=spd.MIN_POLL_BACKLOG_MIN)   # every poll reaches back at least this far


class GapWindow(DaemonTestCase):

    def window(self, daemon, ids=(ID_A,)):
        start, stop, deep = daemon._compute_daemon_window(UTC, NOW, sensors(*ids))
        self.assertTrue(deep)
        self.assertEqual(stop, NOW)
        return start

    def test_outage_is_found_from_the_newest_stored_point(self):
        d = self.make_daemon([fake_writer(newest={TAG_A: NOW - 6 * H})])
        self.assertEqual(self.window(d), NOW - 7 * H)

    def test_empty_database_fetches_the_poll_backlog(self):
        d = self.make_daemon([fake_writer()])
        self.assertEqual(self.window(d), NOW - BACKLOG)

    def test_failed_lookups_resume_from_the_persisted_collection_mark(self):
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'sensors': {}, 'failed_windows': [],
             'collected_until': (NOW - 6 * H).isoformat()}))
        d = self.make_daemon([fake_writer(fail=True)])
        with self.assertLogs('sensorpushd', 'WARNING') as logs:
            start = self.window(d)
        self.assertEqual(start, NOW - 7 * H)
        self.assertIn('Could not read the newest stored point', '\n'.join(logs.output))

    def test_upgrade_adopts_the_old_marker_once(self):
        # Releases without collected_until wrote last-success at the end of
        # a cycle that stored data.  It is copied into state.json on the
        # first start, because from then on the marker is refreshed after
        # every window - also halfway through a long back-fill.
        (self.state_dir / 'last-success').write_text((NOW - 6 * H).isoformat() + '\n')
        first = self.make_daemon([fake_writer()])
        self.assertEqual(first._collected_until, NOW - 6 * H)
        state = json.loads((self.state_dir / 'state.json').read_text())
        self.assertEqual(state['collected_until'], (NOW - 6 * H).isoformat())
        # A back-fill lands its first window (fresh marker), then restarts.
        (self.state_dir / 'last-success').write_text(NOW.isoformat() + '\n')
        d = self.make_daemon([fake_writer(fail=True)])
        with self.assertLogs('sensorpushd', 'WARNING'):
            self.assertEqual(self.window(d), NOW - 7 * H)

    def test_interrupted_backfill_resumes_where_the_database_was_complete(self):
        # The gap check finds the database 5 days behind and starts a long
        # back-fill; the process dies after its first window (fresh
        # last-success marker, cycle never completed); on restart the
        # lookups time out.  Resuming from the marker would drop 4.5 days.
        api = FakeAPI([ID_A])
        first = self.make_daemon([fake_writer(newest={TAG_A: NOW - 120 * H})], api=api)
        original = first._fetch_and_write_window

        def die_after_one(*a, **k):
            ok = original(*a, **k)
            first.running = False
            return ok
        first._fetch_and_write_window = die_after_one
        first._collect_cycle()
        self.assertEqual(len(api.calls), 1)
        self.assertTrue((self.state_dir / 'last-success').is_file())
        d = self.make_daemon([fake_writer(fail=True)])
        with self.assertLogs('sensorpushd', 'WARNING'):
            start, _, _ = d._compute_daemon_window(UTC, NOW, sensors(ID_A))
        self.assertEqual(start, NOW - 121 * H)

    def test_marker_written_by_this_release_is_never_adopted(self):
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'sensors': {}, 'failed_windows': [], 'collected_until': None}))
        (self.state_dir / 'last-success').write_text(NOW.isoformat() + '\n')
        self.assertIsNone(self.make_daemon([fake_writer()])._collected_until)

    def test_naive_collection_mark_is_read_as_utc(self):
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'collected_until': (NOW - 6 * H).replace(tzinfo=None).isoformat()}))
        d = self.make_daemon([fake_writer(fail=True)])
        self.assertEqual(d._collected_until, NOW - 6 * H)
        with self.assertLogs('sensorpushd', 'WARNING'):
            self.assertEqual(self.window(d), NOW - 7 * H)

    def test_skipped_backend_is_not_taken_for_empty(self):
        # Restart after an outage: the first write of the cycle failed while
        # the database was still starting, so the gap check skips it - and
        # the next write would make the outage invisible for good.
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'collected_until': (NOW - 6 * H).isoformat()}))
        w = fake_writer(newest={TAG_A: NOW - 6 * H})
        w._consecutive_failures = 1
        d = self.make_daemon([w])
        self.assertEqual(self.window(d), NOW - 7 * H)
        self.assertEqual(w.lookups, [])

    def test_skipped_backend_does_not_drag_a_healthy_one(self):
        ok = fake_writer(newest={TAG_A: NOW - datetime.timedelta(minutes=4)})
        down = fake_writer()
        down._connected = False
        d = self.make_daemon([ok, down])
        d._fetch_high_water = NOW - datetime.timedelta(minutes=5)
        d._last_deep_scan = None
        self.assertEqual(self.window(d), NOW - BACKLOG)

    def test_only_a_stale_sensor_answering_cannot_drag_the_window_back(self):
        w = fake_writer(newest={TAG_B: NOW - 90 * 24 * H}, fail={TAG_A})
        d = self.make_daemon([w])
        d._collected_until = NOW - 2 * H
        with self.assertLogs('sensorpushd', 'WARNING'):
            self.assertEqual(self.window(d, ids=(ID_A, ID_B)), NOW - 3 * H)

    def test_failed_lookups_with_nothing_on_record_warn_loudly(self):
        d = self.make_daemon([fake_writer(fail=True)])
        with self.assertLogs('sensorpushd', 'WARNING') as logs:
            self.assertEqual(self.window(d), NOW - BACKLOG)
        self.assertIn('no completed collection is on record', '\n'.join(logs.output))

    def test_in_process_high_water_mark_wins(self):
        d = self.make_daemon([fake_writer(fail=True)])
        d._fetch_high_water = NOW - 2 * H
        d._collected_until = NOW - 9 * H
        with self.assertLogs('sensorpushd', 'WARNING'):
            self.assertEqual(self.window(d), NOW - 3 * H)

    def test_partial_failure_uses_the_sensors_that_answered(self):
        w = fake_writer(newest={TAG_B: NOW - 3 * H}, fail={TAG_A})
        d = self.make_daemon([w])
        with self.assertLogs('sensorpushd', 'WARNING') as logs:
            start = self.window(d, ids=(ID_A, ID_B))
        self.assertEqual(start, NOW - 4 * H)
        self.assertIn('1 of 2 sensor(s)', '\n'.join(logs.output))
        self.assertEqual(set(d._sensor_last_stored), {ID_B})

    def test_sensorlist_limits_the_gap_check(self):
        w = fake_writer(newest={TAG_A: NOW - H, TAG_B: NOW - 30 * H})
        d = self.make_daemon([w], args=make_args(sensorlist=[ID_A]))
        self.window(d, ids=(ID_A, ID_B))
        self.assertEqual([sid for _, sid in w.lookups], [TAG_A])
        self.assertEqual(set(d._sensor_last_stored), {ID_A})

    def test_lookups_use_the_measurement_that_is_written(self):
        w1, w2 = fake_writer('SensorPush'), fake_writer('Other')
        with self.assertLogs('sensorpushd', 'WARNING') as logs:
            d = self.make_daemon([w1, w2])
        self.assertIn('MEASUREMENT_NAME differs', '\n'.join(logs.output))
        self.window(d)
        self.assertEqual({m for m, _ in w1.lookups + w2.lookups}, {'SensorPush'})

    def test_lookups_use_the_stored_id_form(self):
        w = fake_writer()
        self.window(self.make_daemon([w]), ids=(ID_A, ID_C))
        self.assertEqual([sid for _, sid in w.lookups],
                         [str(float(ID_A)), str(float(ID_C))])


class PollBacklog(DaemonTestCase):
    """Every look-back window reaches back at least MIN_POLL_BACKLOG_MIN.

    The SensorPush API returns each sensor's samples in ~15-minute chunks
    and selects a chunk by its start time, so a short window misses data.
    """

    def test_short_setting_is_raised_with_one_warning(self):
        d = self.make_daemon([fake_writer()], poll_backlog='10m')
        with self.assertLogs('sensorpushd', 'WARNING') as logs:
            start, _, _ = d._compute_daemon_window(UTC, NOW, sensors(ID_A))
        self.assertEqual(start, NOW - BACKLOG)
        self.assertIn('POLL_BACKLOG = 10m is raised to 60 min', '\n'.join(logs.output))
        d._last_deep_scan = None
        with self.assertNoLogs('sensorpushd', 'WARNING'):
            d._compute_daemon_window(UTC, NOW, sensors(ID_A))

    def test_default_and_longer_settings_are_kept(self):
        self.assertEqual(spd.DEFAULT_DAEMON_CFG['poll_backlog'], '1h')
        d = self.make_daemon([fake_writer()], poll_backlog='2h')
        start, _, _ = d._compute_daemon_window(UTC, NOW, sensors(ID_A))
        self.assertEqual(start, NOW - 2 * H)

    def test_incremental_poll_reaches_back_an_hour(self):
        d = self.make_daemon([fake_writer()], poll_backlog='10m')
        d._fetch_high_water = NOW - datetime.timedelta(minutes=5)
        d._last_deep_scan = time.monotonic()
        with self.assertLogs('sensorpushd', 'WARNING'):
            start, _, deep = d._compute_daemon_window(UTC, NOW, sensors(ID_A))
        self.assertFalse(deep)
        self.assertEqual(start, NOW - BACKLOG)

    def test_oneshot_backlog_is_raised(self):
        d = self.make_daemon([fake_writer()], args=make_args(daemon=False, backlog='10m'))
        with self.assertLogs('sensorpushd', 'WARNING'):
            start, _ = d._compute_oneshot_window(UTC, NOW)
        self.assertEqual(start, NOW - BACKLOG)

    def simulate(self, hours=4, chunk=900, delay=120):
        """Poll every 5 min against an API that answers in chunks.

        One sample a minute per sensor; each sensor's chunks start at its
        own phase; a sample is visible ``delay`` s after it was observed; a
        query from T returns only chunks that started at or after T.
        Returns the number of settled samples that never got stored.
        """
        t0 = int(NOW.timestamp()) - hours * 3600
        phases = {ID_A: 0, ID_B: 300, ID_C: 610}
        cloud = {(sid, t0 + k * 60 + p % 60) for sid, p in phases.items()
                 for k in range(hours * 60)}
        chunk_start = lambda sid, t: t - (t - phases[sid]) % chunk
        w = fake_writer()
        d = self.make_daemon([w], poll_backlog='10m')
        stored = set()
        for now in range(t0 + 3600, int(NOW.timestamp()) + 1, 300):
            at = datetime.datetime.fromtimestamp(now, UTC)
            w.newest = {spd.sensor_id_tag(sid): datetime.datetime.fromtimestamp(t, UTC)
                        for sid, t in stored}
            start, _, _ = d._compute_daemon_window(UTC, at, sensors(*phases))
            stored |= {(sid, t) for sid, t in cloud
                       if t + delay <= now and chunk_start(sid, t) >= start.timestamp()}
            d._fetch_high_water = at
        settled = {(sid, t) for sid, t in cloud
                   if t0 + 3600 <= t <= NOW.timestamp() - 1800}
        return len(settled - stored)

    def test_no_sample_is_lost_with_chunked_answers(self):
        with self.assertLogs('sensorpushd', 'WARNING'):
            self.assertEqual(self.simulate(), 0)

    def test_simulation_catches_the_old_ten_minute_window(self):
        floor = spd.MIN_POLL_BACKLOG_MIN
        spd.MIN_POLL_BACKLOG_MIN = 0          # behave like before the fix
        try:
            missing = self.simulate()
        finally:
            spd.MIN_POLL_BACKLOG_MIN = floor
        self.assertGreater(missing, 100)


class Persistence(DaemonTestCase):

    def test_cycle_persists_collection_mark_and_success_marker(self):
        w = fake_writer()
        d = self.make_daemon([w], api=FakeAPI([ID_A]))
        d._collect_cycle()
        self.assertTrue(w.written)
        state = json.loads((self.state_dir / 'state.json').read_text())
        self.assertEqual(datetime.datetime.fromisoformat(state['collected_until']),
                         d._fetch_high_water)
        self.assertTrue((self.state_dir / 'last-success').is_file())
        # ... and a fresh process picks the mark up again.
        self.assertEqual(self.make_daemon([fake_writer()])._collected_until,
                         d._fetch_high_water)

    def test_dry_run_leaves_state_and_marker_alone(self):
        (self.state_dir / 'state.json').write_text(json.dumps(
            {'sensors': {ID_A: {'gap_from': (NOW - 50 * H).isoformat(),
                                'returned_at': 0}},
             'failed_windows': []}))
        before = (self.state_dir / 'state.json').read_text()
        w = fake_writer()
        d = self.make_daemon([w], api=FakeAPI([ID_A]), args=make_args(dryrun=True))
        d._collect_cycle()
        d._save_state()
        self.assertEqual(w.written, [])
        self.assertEqual((self.state_dir / 'state.json').read_text(), before)
        self.assertFalse((self.state_dir / 'last-success').exists())

    def test_failed_save_leaves_no_temp_file(self):
        d = self.make_daemon([fake_writer()])
        original = spd.json.dump

        def broken(*_a, **_k):
            raise OSError('No space left on device')
        spd.json.dump = broken
        try:
            with self.assertLogs('sensorpushd', 'WARNING'):
                d._save_state()
        finally:
            spd.json.dump = original
        self.assertEqual(list(self.state_dir.glob('.state-*.tmp')), [])

    def test_success_marker_is_refreshed_after_every_window(self):
        d = self.make_daemon([fake_writer()], api=FakeAPI([ID_A]),
                             args=make_args(daemon=False, backlog='3d'))
        marks = []
        d._write_last_success = lambda: marks.append(d._last_successful_time)
        d._collect_cycle()
        windows = len(spd.build_timelist(NOW - 72 * H, NOW, 720))
        self.assertEqual(len(marks), windows)


class SensorUid(DaemonTestCase):

    def records(self, d, *ids):
        d._note_sensors(sensors(*ids))
        return spd.build_voltage_records(sensors(*ids), 'SensorPush', NOW)

    def test_off_by_default_and_untouched(self):
        w = fake_writer()
        d = self.make_daemon([w])
        recs = self.records(d, ID_A)
        self.assertFalse(w.sensor_uid)
        self.assertIs(d._records_for(w, recs), recs)

    def test_on_adds_the_full_id_and_keeps_sensor_id(self):
        plain, uid = fake_writer(), fake_writer(sensor_uid=True)
        d = self.make_daemon([plain, uid])
        recs = self.records(d, ID_A, ID_B)
        d._safe_write(recs)
        self.assertIs(plain.written[0], recs)
        for r, original in zip(uid.written[0], recs):
            self.assertEqual(r['tags']['sensor_uid'],
                             ID_A if original['tags']['sensor_id'] == TAG_A else ID_B)
            self.assertEqual(r['tags']['sensor_id'], original['tags']['sensor_id'])
            self.assertNotIn('sensor_uid', original['tags'])

    def test_collisions_are_reported_once_and_still_get_the_right_uid(self):
        a, b = '123456.78901234567890', '123456.78901234567891'
        uid = fake_writer(sensor_uid=True)
        d = self.make_daemon([uid])
        with self.assertLogs('sensorpushd', 'ERROR'):
            d._note_sensors(sensors(a, b, ID_C))
        with self.assertNoLogs('sensorpushd', 'ERROR'):
            d._note_sensors(sensors(a, b, ID_C))
        recs = spd.build_voltage_records(sensors(a, b, ID_C), 'SensorPush', NOW)
        got = {r['tags']['sensor_name']: r['tags'].get('sensor_uid')
               for r in d._records_for(uid, recs)}
        self.assertEqual(got, {'S0': a, 'S1': b, 'S2': ID_C})

    def test_collision_with_the_same_name_gets_no_uid(self):
        a, b = '123456.78901234567890', '123456.78901234567891'
        same = {a: {'id': a, 'name': 'Twin'}, b: {'id': b, 'name': 'Twin'}}
        uid = fake_writer(sensor_uid=True)
        d = self.make_daemon([uid])
        with self.assertLogs('sensorpushd', 'ERROR'):
            d._note_sensors(same)
        recs = spd.build_voltage_records(same, 'SensorPush', NOW)
        self.assertTrue(all('sensor_uid' not in r['tags']
                            for r in d._records_for(uid, recs)))


if __name__ == '__main__':
    unittest.main()
