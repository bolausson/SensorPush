"""Gap-detection lookups of the three backends.

Contract (BaseWriter.get_last_timestamp): return the newest stored point,
None when there is no matching data, and raise when the backend could not
be asked - a failed lookup must never look like "no data".
"""
import datetime
import types
import unittest

import requests

from helpers import ID_A, sensorpushd

spd = sensorpushd()
TAG = spd.sensor_id_tag(ID_A)
NOW = 1794148737             # evaluation time VictoriaMetrics puts in value[0]
SAMPLE = 1772197662          # time of the newest stored sample


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status = payload, status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f'{self.status} error')

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response, self.queries = response, []

    def get(self, url, params=None, verify=None, timeout=None):
        self.queries.append(params['query'])
        return self.response


def vm_writer(response):
    w = spd.VMWriter({'url': 'http://vm.invalid', 'verify_ssl': True,
                      'measurement_name': 'SensorPush'})
    w.session = FakeSession(response)
    w._connected = True
    return w


def vm_result(*sample_times):
    # Documented instant-query shape: value = [evaluation time, value]; for
    # tlast_over_time() the value is the newest sample's unix time.
    return {'status': 'success', 'data': {'resultType': 'vector', 'result': [
        {'metric': {'sensor_id': TAG, 'sensor_name': f'n{i}'},
         'value': [NOW, str(t)]} for i, t in enumerate(sample_times)]}}


class VictoriaMetrics(unittest.TestCase):

    def test_reads_sample_time_not_evaluation_time(self):
        w = vm_writer(FakeResponse(vm_result(SAMPLE)))
        ts = w.get_last_timestamp('SensorPush', sensor_id=TAG)
        self.assertEqual(ts.timestamp(), SAMPLE)
        self.assertEqual(
            w.session.queries,
            [f'tlast_over_time(SensorPush_temperature{{sensor_id="{TAG}"}}[10y])'])

    def test_newest_of_several_series(self):
        w = vm_writer(FakeResponse(vm_result(SAMPLE - 3600, SAMPLE, SAMPLE - 60)))
        self.assertEqual(w.get_last_timestamp('SensorPush', TAG).timestamp(), SAMPLE)

    def test_no_data(self):
        w = vm_writer(FakeResponse(vm_result()))
        self.assertIsNone(w.get_last_timestamp('SensorPush', TAG))

    def test_lookback_bound(self):
        w = vm_writer(FakeResponse(vm_result(SAMPLE)))
        w.get_last_timestamp('SensorPush', TAG, lookback_min=90)
        self.assertTrue(w.session.queries[0].endswith('[90m])'))

    def test_errors_raise(self):
        with self.assertRaises(requests.HTTPError):
            vm_writer(FakeResponse({}, status=422)).get_last_timestamp('SensorPush', TAG)
        with self.assertRaises(RuntimeError):
            vm_writer(FakeResponse({'status': 'error', 'error': 'boom'})
                      ).get_last_timestamp('SensorPush', TAG)


class FakeTable:
    def __init__(self, value):
        self.value = value

    def __len__(self):
        return 1

    def column(self, _name):
        return [types.SimpleNamespace(as_py=lambda: self.value)]


class FakeClient3:
    def __init__(self, *answers):
        self.answers, self.queries = list(answers), []

    def query(self, query):
        self.queries.append(query)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return FakeTable(answer)


def influx3(*answers):
    w = spd.InfluxDB3Writer({'url': 'http://i3.invalid', 'database': 'db',
                             'token': 't', 'measurement_name': 'SensorPush'})
    w.client = FakeClient3(*answers)
    w._connected = True
    return w


class InfluxDB3(unittest.TestCase):
    T = datetime.datetime(2026, 11, 8, 12, 0)

    def test_recent_point_found_in_first_window(self):
        w = influx3(self.T)
        self.assertEqual(w.get_last_timestamp('SensorPush', TAG), self.T)
        self.assertEqual(w.client.queries, [
            'SELECT max(time) AS last_time FROM "SensorPush" WHERE '
            f"time > now() - INTERVAL '1440 minutes' AND sensor_id = '{TAG}'"])

    def test_widens_then_goes_unbounded(self):
        w = influx3(None, None, None, self.T)
        self.assertEqual(w.get_last_timestamp('SensorPush', TAG), self.T)
        self.assertIn("'2880 minutes'", w.client.queries[1])
        self.assertIn("'43200 minutes'", w.client.queries[2])
        self.assertEqual(w.client.queries[3],
                         'SELECT max(time) AS last_time FROM "SensorPush" '
                         f"WHERE sensor_id = '{TAG}'")

    def test_error_in_a_wider_window_raises(self):
        # e.g. the file limit hit by the 30-day query: a failed lookup,
        # never "no data".
        w = influx3(None, None, RuntimeError('would scan more than 432 files'))
        with self.assertRaises(RuntimeError):
            w.get_last_timestamp('SensorPush', TAG)

    def test_missing_table_or_database_messages(self):
        for msg in ("Error during planning: table 'public.iox.SensorPush' not found",
                    'database not found: sensorpush',
                    'Database "sensorpush" not found',
                    'namespace sensorpush not found'):
            self.assertTrue(spd.InfluxDB3Writer._is_missing(RuntimeError(msg)), msg)
        for msg in ('parquet file not found in object store for table SensorPush',
                    'object store: not found: path /x/y.parquet',
                    'connection refused'):
            self.assertFalse(spd.InfluxDB3Writer._is_missing(RuntimeError(msg)), msg)

    def test_explicit_lookback_is_one_query(self):
        w = influx3(self.T)
        w.get_last_timestamp('SensorPush', TAG, lookback_min=60)
        self.assertEqual(len(w.client.queries), 1)
        self.assertIn("'60 minutes'", w.client.queries[0])

    def test_missing_table_is_no_data(self):
        missing = RuntimeError("table 'SensorPush' not found")
        w = influx3(missing, missing, missing, missing)
        self.assertIsNone(w.get_last_timestamp('SensorPush', TAG))

    def test_other_errors_raise(self):
        with self.assertRaises(RuntimeError):
            influx3(RuntimeError('query would scan 900 parquet files')
                    ).get_last_timestamp('SensorPush', TAG, lookback_min=60)


class FakeQueryApi:
    def __init__(self, tables=(), exc=None):
        self.tables, self.exc, self.queries = list(tables), exc, []

    def query(self, query, org=None):
        self.queries.append(query)
        if self.exc:
            raise self.exc
        return self.tables


def influx2(api):
    w = spd.InfluxDB2Writer({'url': 'http://i2.invalid', 'token': 't', 'org': 'o',
                             'bucket': 'b', 'verify_ssl': True,
                             'measurement_name': 'SensorPush'})
    w.query_api = api
    w._connected = True
    return w


def table(*times):
    return types.SimpleNamespace(records=[
        types.SimpleNamespace(get_time=lambda t=t: t) for t in times])


class InfluxDB2(unittest.TestCase):

    def test_newest_row_across_series(self):
        a = datetime.datetime(2026, 11, 8, 10, tzinfo=datetime.timezone.utc)
        b = a + datetime.timedelta(hours=1)
        w = influx2(FakeQueryApi([table(a), table(b)]))
        self.assertEqual(w.get_last_timestamp('SensorPush', TAG), b)
        self.assertIn(f'r.sensor_id == "{TAG}"', w.query_api.queries[0])

    def test_no_data(self):
        self.assertIsNone(influx2(FakeQueryApi([])).get_last_timestamp('SensorPush', TAG))

    def test_errors_raise(self):
        with self.assertRaises(TimeoutError):
            influx2(FakeQueryApi(exc=TimeoutError('read timed out'))
                    ).get_last_timestamp('SensorPush', TAG)


if __name__ == '__main__':
    unittest.main()
