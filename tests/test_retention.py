"""InfluxDB 2 bucket retention.

A write with points older than the bucket keeps gets HTTP 422 "partial
write: points beyond retention policy dropped=N": InfluxDB 2 stored the
others, and the dropped ones can never be stored, so it counts as a
completed write.  At startup the daemon warns if the bucket keeps no longer
than the last RECHECK_GAPS time.
"""
import json
import logging
import types
import unittest

from helpers import ID_A, ID_B, fake_writer, make_args, make_config, sensorpushd
import test_rechecks as tr

spd = sensorpushd()
M, H, D, T0 = tr.M, tr.H, tr.D, tr.T0

try:
    from influxdb_client.rest import ApiException
except ImportError:
    class ApiException(Exception):
        """What influxdb_client.rest.ApiException (1.39) carries, for a
        test run without influxdb-client."""

        def __init__(self, status=None, reason=None, http_resp=None):
            self.status, self.reason = http_resp.status, http_resp.reason
            self.body, self.headers = http_resp.data, http_resp.getheaders()
            try:
                self.message = json.loads(self.body)['message']
            except (TypeError, ValueError, KeyError):
                self.message = self.body
            super().__init__(self.message)

        def __str__(self):
            return '(%s)\nReason: %s\nHTTP response body: %s\n' % (
                self.status, self.reason, self.body)


class Answer:
    """The HTTP answer influxdb-client builds its ApiException from."""

    def __init__(self, status, data, reason='Unprocessable Entity'):
        self.status, self.reason, self.data = status, reason, data

    def getheaders(self):
        return {}

    def getheader(self, name, default=None):
        return default


def error(status, message):
    body = json.dumps({'code': 'unprocessable entity', 'message': message}).encode()
    return ApiException(http_resp=Answer(status, body))


BEYOND = ('failure writing points to database: partial write: points beyond '
          'retention policy dropped=%d')
CONFLICT = ('failure writing points to database: partial write: field type '
            'conflict: input field "temperature" on measurement "SensorPush" is '
            'type integer, already exists as type float dropped=1')


class FakeWriteApi:
    """Takes a write, or raises the next of ``answers`` that is not None."""

    def __init__(self, *answers, then=None):
        self.answers, self.then, self.calls = list(answers), then, []

    def write(self, bucket=None, org=None, record=None):
        self.calls.append(list(record))
        answer = self.answers.pop(0) if self.answers else self.then
        if answer is not None:
            raise answer


class FakeQueryApi:
    """The newest stored point of every sensor: ``newest``."""

    def __init__(self, newest):
        self.newest = newest

    def query(self, query, org=None):
        return [types.SimpleNamespace(records=[
            types.SimpleNamespace(get_time=lambda: self.newest)])]


ORG_ID = '0123456789abcdef'


class FakeClient:
    """An InfluxDBClient whose buckets API finds ``bucket`` (or raises it)
    in the writer's organization, 'o' (ID ORG_ID); any other organization
    is not found (404).  ``others`` are buckets of that name in other
    organizations, which a lookup by the name alone lists first (with a
    token that reads every organization's buckets).  ``asked`` records the
    lookups."""

    def __init__(self, bucket, others=()):
        self.bucket, self.others, self.asked = bucket, list(others), []

    def buckets_api(self):
        return self

    def find_buckets(self, name=None, org=None, org_id=None):
        self.asked.append((name, org or org_id))
        if isinstance(self.bucket, Exception):
            raise self.bucket
        if (org, org_id) not in (('o', None), (None, ORG_ID)):
            raise ApiException(http_resp=Answer(
                404, b'{"code": "not found", "message": "organization not found"}',
                'Not Found'))
        return types.SimpleNamespace(buckets=[b for b in [self.bucket] if b is not None])

    def find_bucket_by_name(self, name):
        self.asked.append(name)
        if isinstance(self.bucket, Exception):
            raise self.bucket
        return (self.others + [self.bucket])[0]

    def close(self):
        pass


def bucket(*rules):
    return types.SimpleNamespace(name='sensorpush', retention_rules=[
        types.SimpleNamespace(type=t, every_seconds=s) for t, s in rules])


def influx2(write_api=None, client=None, newest=None):
    w = spd.InfluxDB2Writer({'url': 'http://i2.invalid', 'token': 't', 'org': 'o',
                             'bucket': 'sensorpush', 'verify_ssl': True,
                             'measurement_name': 'SensorPush'})
    w.write_api, w.client = write_api or FakeWriteApi(), client
    w.query_api = FakeQueryApi(newest)
    w._connected = True
    return w


def records(*times):
    return [{'measurement': 'SensorPush', 'tags': {'sensor_id': 'x', 'sensor_name': 'S0'},
             'fields': {'temperature': 20.0}, 'time': tr.iso(t)} for t in times]


class Writer(tr.RecheckCase):

    def test_points_beyond_the_retention_count_as_a_completed_write(self):
        class Text(Exception):
            status = 422

            def __str__(self):
                return '(422) ' + BEYOND % 1
        only_message = ApiException(http_resp=Answer(422, None))
        only_message.message = BEYOND % 2
        str_body = error(422, BEYOND % 1)
        str_body.body = str_body.body.decode()
        no_count = error(422, 'partial write: points beyond retention policy')
        answers = [error(422, BEYOND % 2), str_body, only_message, Text(), no_count]
        w = influx2(FakeWriteApi(*answers))
        for _ in answers:
            w.write(records(T0 - 20 * D, T0 - 19 * D))
        self.assertEqual(len(w.write_api.calls), len(answers))
        span = spd._fmt_span(T0 - 20 * D, T0 - 19 * D, T0)
        warnings = self.logged(logging.WARNING)
        self.assertEqual(warnings, [
            "InfluxDB 2 bucket 'sensorpush' did not store 2 of the 2 points written for %s: "
            "they are older than the bucket keeps (its retention), so it dropped them all. "
            "Such a write counts as done; later ones are logged at INFO" % span])
        self.assertEqual(self.logged(logging.INFO, 'did not store'), [
            "InfluxDB 2 bucket 'sensorpush' did not store %s of the 2 points written for %s: "
            "older than the bucket keeps" % (n, span) for n in ('1', '2', '1', 'some')])

    def test_the_warning_says_what_the_bucket_stored(self):
        # All of the points, some of them, or a count the answer leaves out.
        for message, kept in ((BEYOND % 3, 'dropped them all'),
                              (BEYOND % 1, 'dropped them and stored the others'),
                              ('partial write: points beyond retention policy',
                               'dropped them and stored the others, if any')):
            with self.subTest(kept):
                del self.records[:]
                influx2(FakeWriteApi(error(422, message))).write(
                    records(T0 - 20 * D, T0 - 19 * D, T0))
                self.assertEqual(len(self.logged(logging.WARNING)), 1)
                self.assertIn('of the 3 points written for %s: they are older than the bucket '
                              'keeps (its retention), so it %s. Such a write counts as done'
                              % (spd._fmt_span(T0 - 20 * D, T0, T0), kept),
                              self.logged(logging.WARNING)[0])

    def test_every_other_error_is_raised(self):
        # Another 422, the retention text with another status, no answer.
        for exc in (error(422, CONFLICT), error(422, 'unprocessable entity'),
                    error(500, BEYOND % 1),
                    ApiException(http_resp=Answer(503, b'', 'Service Unavailable')),
                    ConnectionError('connection refused'), TimeoutError('timed out')):
            with self.subTest(exc=type(exc).__name__):
                w = influx2(FakeWriteApi(exc))
                with self.assertRaises(type(exc)):
                    w.write(records(T0))
        self.assertEqual(self.logged(logging.WARNING), [])

    def test_bucket_retention(self):
        for found, keeps in ((bucket(('expire', 7 * D)), 7 * D), (bucket(('expire', 0)), 0),
                             (bucket(), 0), (bucket((None, 3 * D)), 3 * D),
                             (bucket(('expire', 0), ('expire', 30 * D)), 30 * D)):
            self.assertEqual(influx2(client=FakeClient(found)).bucket_retention(), keeps)
        for found in (None, ApiException(http_resp=Answer(401, b'{"message": "unauthorized"}'))):
            with self.assertRaises(Exception):
                influx2(client=FakeClient(found)).bucket_retention()

    def test_the_bucket_is_looked_up_in_its_organization(self):
        # Another organization has a bucket of that name too, which a token
        # that reads every organization's buckets lists first by the name
        # alone: its retention is not this bucket's.
        other = bucket(('expire', H))
        client = FakeClient(bucket(('expire', 7 * D)), others=[other])
        self.assertEqual(influx2(client=client).bucket_retention(), 7 * D)
        self.assertEqual(client.asked, [('sensorpush', 'o')])
        # Not in ORG at all: no bucket, whatever other organizations hold.
        client = FakeClient(None, others=[other])
        with self.assertRaises(LookupError):
            influx2(client=client).bucket_retention()
        self.assertEqual(client.asked, [('sensorpush', 'o')])
        # ORG may be the organization's ID, which the writes take too.
        client = FakeClient(bucket(('expire', 7 * D)), others=[other])
        w = influx2(client=client)
        w.org = ORG_ID
        self.assertEqual(w.bucket_retention(), 7 * D)
        self.assertEqual(client.asked, [('sensorpush', ORG_ID)])
        # One the lookup cannot use: by the bucket's name alone, as before.
        client = FakeClient(bucket(('expire', 7 * D)))
        w = influx2(client=client)
        w.org = 'my org'
        self.assertEqual(w.bucket_retention(), 7 * D)
        self.assertEqual(client.asked, [('sensorpush', 'my org'), 'sensorpush'])


class Daemon(tr.RecheckCase):
    """The write paths: _safe_write, the polls and the gap re-reads."""

    def setUp(self):
        super().setUp()
        self.cloud = tr.Cloud(self.clock, {ID_A: 'HT.w'})
        self.sensors = self.cloud.sensors()
        self.cloud.add(ID_A, tr.every(T0 - 30 * D, T0 + D, 10 * M))

    def make(self, write_api, **daemon_cfg):
        w = influx2(write_api, newest=tr.at(T0 - 3 * M))
        d = self.make_daemon([w], api=self.cloud, **daemon_cfg)
        d._interruptible_sleep = self.sleep
        return d, w

    def test_safe_write_takes_it_as_written(self):
        d, w = self.make(FakeWriteApi(error(422, BEYOND % 2)))
        self.assertTrue(d._safe_write(records(T0 - 20 * D, T0)))
        # No second attempt, no failure counted, no reconnect.
        self.assertEqual(len(w.write_api.calls), 1)
        self.assertEqual((w._consecutive_failures, w.connected), (0, True))
        self.assertTrue(d._writers_healthy())
        self.assertTrue(self.logged(logging.INFO, 'Write to InfluxDB2Writer OK'))
        self.assertEqual(self.logged(logging.ERROR), [])
        # Another 422 fails as before: two attempts, one failure.
        d, w = self.make(FakeWriteApi(then=error(422, CONFLICT)))
        self.assertFalse(d._safe_write(records(T0)))
        self.assertEqual((len(w.write_api.calls), w._consecutive_failures), (2, 1))

    def test_polls_record_no_failed_window(self):
        # The poll's write of a cycle - its voltage records go first - is
        # answered with the retention 422: the window landed.
        d, w = self.make(FakeWriteApi(None, error(422, BEYOND % 7)))
        d._collect_cycle()
        self.assertEqual(len(w.write_api.calls), 2)
        self.assertEqual(d._failed_windows, [])
        self.assertIsNotNone(d._fetch_high_water)
        self.assertTrue((self.state_dir / 'last-success').exists())
        # With a type conflict the window is recorded for a retry, as before.
        self.reset_state()
        d, w = self.make(FakeWriteApi(None, then=error(422, CONFLICT)))
        d._collect_cycle()
        self.assertEqual(len(d._failed_windows), 1)
        self.assertIsNone(d._fetch_high_water)

    def test_gap_re_reads_go_on_at_the_usual_pace(self):
        # A 20-day gap of an HT.w, 44 cells; the bucket keeps 14 days, so
        # InfluxDB 2 drops the points of the oldest cells.  Each such write
        # counts as done: the gap is read cell by cell, together, without a
        # failed call, a back-off or a refused cell, and the writers gate
        # stays open.  (Refused, each old cell cost a failed call read alone,
        # about one an hour.)
        api = FakeWriteApi()
        d, w = self.make(api)
        real_write, dropped = api.write, []

        def write(bucket=None, org=None, record=None):
            old = [r for r in record
                   if spd._observed_epoch(r['time']) < self.clock.t - 14 * D]
            real_write(bucket, org, record)
            if old:
                dropped.append(len(old))
                raise error(422, BEYOND % len(old))
        api.write = write
        d._register_hole(ID_A, T0 - 20 * D, T0 - H, self.sensors, now=T0)
        hole = d._recheck_holes[0]
        cells = spd._hole_ncells(hole)
        self.assertEqual(cells, 44)
        self.clock.advance(H)
        for _ in range(8):
            d._recheck_gaps(self.sensors, 'SensorPush')
            self.assertNotIn('_fail', hole)
            self.clock.advance(5 * M)
        self.assertEqual(len(self.cloud.sample_calls()), cells)
        self.assertEqual((hole['done'], hole['pos']), (1, 0))
        self.assertEqual((d._recheck_fail_streak, d._recheck_alone_streak), (0, 0))
        self.assertIsNone(d._recheck_next_at)
        self.assertTrue(d._writers_healthy())
        self.assertGreater(len(dropped), 10)
        self.assertEqual(len(self.logged(logging.WARNING, 'did not store')), 1)
        self.assertEqual(len(self.logged(logging.INFO, 'did not store')), len(dropped) - 1)
        self.assertEqual(self.logged(logging.WARNING, 'Writing gap re-reads failed'), [])

    def test_another_refusal_still_fails_the_re_read(self):
        d, w = self.make(FakeWriteApi(then=error(422, CONFLICT)))
        d._register_hole(ID_A, T0 - 3 * H, T0 - 2 * H, self.sensors, now=T0)
        hole = d._recheck_holes[0]
        self.clock.advance(H)
        d._recheck_gaps(self.sensors, 'SensorPush')
        self.assertEqual((hole['done'], d._recheck_fail_streak), (0, 1))
        self.assertTrue(hole['_fail']['refused'])
        self.assertEqual(len(self.logged(logging.WARNING, 'Writing gap re-reads failed')), 1)


class StartupCheck(tr.RecheckCase):
    """The bucket's retention, read once at startup (B2)."""

    def check(self, found, args=None, **daemon_cfg):
        del self.records[:]
        client = FakeClient(found)
        w = influx2(client=client)
        d = self.make_daemon([w, fake_writer()], api=tr.Cloud(self.clock, {}),
                             args=args or make_args(), **daemon_cfg)
        d._check_bucket_retention()
        return client, self.logged(logging.WARNING)

    def test_a_bucket_that_keeps_no_longer_than_the_last_re_read(self):
        for keeps, advice in (
                (7 * D, 'Set RECHECK_GAPS = 1h, 6h, 1d, 3d to re-read gaps only within '
                        'the retention'),
                (6 * H, 'Set RECHECK_GAPS = 1h to re-read gaps only within the retention'),
                (H, 'No RECHECK_GAPS time is within the retention; RECHECK_GAPS = 0 turns '
                    'the re-checks off')):
            with self.subTest(keeps=keeps):
                client, warnings = self.check(bucket(('expire', keeps)))
                self.assertEqual(client.asked, [('sensorpush', 'o')])
                self.assertEqual(warnings, [
                    "InfluxDB 2 bucket 'sensorpush' keeps %s, and RECHECK_GAPS re-reads gaps "
                    "up to 7d after they close: what a re-read writes that late is older than "
                    "the bucket keeps, and InfluxDB 2 drops it. %s"
                    % (spd._fmt_offset(keeps), advice)])

    def test_a_bucket_that_keeps_longer_or_everything(self):
        for found in (bucket(('expire', 7 * D + 1)), bucket(('expire', 0)), bucket()):
            self.assertEqual(self.check(found)[1], [])
        # A schedule of its own: the last offset counts.
        self.assertEqual(self.check(bucket(('expire', 2 * D)), recheck_gaps='1h, 1d')[1], [])
        self.assertTrue(self.check(bucket(('expire', D)), recheck_gaps='1h, 1d')[1])

    def test_a_failed_lookup_is_one_debug_line(self):
        for found in (ApiException(http_resp=Answer(401, b'{"message": "unauthorized"}')),
                      None, AttributeError("'NoneType' object has no attribute 'buckets_api'")):
            client, warnings = self.check(found)
            self.assertEqual(warnings, [])
            debug = self.logged(logging.DEBUG, 'Could not read the retention')
            self.assertEqual(len(debug), 1)

    def test_only_with_gap_re_checks_on_in_daemon_mode(self):
        for kw in ({'recheck_gaps': '0'}, {'args': make_args(daemon=False)}):
            client, warnings = self.check(bucket(('expire', H)), **kw)
            self.assertEqual((client.asked, warnings), ([], []))

    def test_run_checks_once_at_startup(self):
        client = FakeClient(bucket(('expire', 3 * D)))
        w = influx2(client=client)
        d = self.make_daemon([w], api=tr.Cloud(self.clock, {}))
        d._connect_writers = lambda: None
        d._interruptible_sleep = lambda _s: None

        def one_cycle():
            d.running = False
        d._collect_cycle = one_cycle
        self.assertEqual(d.run(), 0)
        self.assertEqual(client.asked, [('sensorpush', 'o')])
        self.assertEqual(len(self.logged(logging.WARNING, 'keeps 3d')), 1)


if __name__ == '__main__':
    unittest.main()
