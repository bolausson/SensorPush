"""The stored sensor_id must stay byte-for-byte what earlier releases wrote.

Earlier releases put float(<API id>) into the record; the writers turned it
into str(float(id)).  sensor_id_tag() now produces that string up front.
These tests pin the value and prove every backend serialises it to the
same bytes as before.
"""
import json
import math
import unittest

from helpers import ID_A, ID_B, ID_C, sensorpushd

spd = sensorpushd()

# The two examples given in GitHub issue #9.
ISSUE_EXAMPLES = {
    '123456.7890123456789012': '123456.78901234567',
    '12345678.9012345678901': '12345678.901234567',
}

try:
    from influxdb_client import Point as Point2
    from influxdb_client.domain.write_precision import WritePrecision as WP2
except ImportError:  # pragma: no cover
    Point2 = None
try:
    from influxdb_client_3.write_client.client.write.point import Point as Point3
    from influxdb_client_3.write_client.domain.write_precision import WritePrecision as WP3
except ImportError:  # pragma: no cover
    Point3 = None


def record(tag_value, name='Kitchen'):
    return {'measurement': 'SensorPush',
            'tags': {'sensor_id': tag_value, 'sensor_name': name},
            'fields': {'temperature': 21.5, 'humidity': 40.0},
            'time': '2026-11-08T12:00:00+0100'}


class SensorIdTag(unittest.TestCase):

    def test_issue_examples(self):
        for api_id, stored in ISSUE_EXAMPLES.items():
            self.assertEqual(spd.sensor_id_tag(api_id), stored)

    def test_same_as_str_float(self):
        for api_id in (ID_A, ID_B, ID_C, *ISSUE_EXAMPLES):
            self.assertEqual(spd.sensor_id_tag(api_id), str(float(api_id)))

    def test_device_id_survives(self):
        for api_id in (ID_A, ID_B, ID_C, *ISSUE_EXAMPLES):
            self.assertEqual(spd.sensor_id_tag(api_id).split('.')[0],
                             api_id.split('.')[0])

    def test_round_trip_is_stable(self):
        # Converting an already stored value again must not change it.
        for api_id in (ID_A, ID_B, ID_C):
            tag = spd.sensor_id_tag(api_id)
            self.assertEqual(spd.sensor_id_tag(tag), tag)

    def test_non_numeric_id_is_stored_unchanged(self):
        # Reported once per id, instead of failing the whole batch.
        for odd in ('abc-123', 'inf'):
            with self.assertLogs('sensorpushd', 'ERROR'):
                self.assertEqual(spd.sensor_id_tag(odd), odd)
        self.assertFalse(math.isfinite(float('inf')))

    def test_collisions(self):
        self.assertEqual(spd.find_sensor_id_collisions([ID_A, ID_B, ID_C]), {})
        # Differ only beyond what a double can hold -> same stored value.
        a, b = '123456.78901234567890', '123456.78901234567891'
        self.assertEqual(spd.find_sensor_id_collisions([a, b, ID_A]),
                         {spd.sensor_id_tag(a): [a, b]})


class SerialisationUnchanged(unittest.TestCase):
    """str tag (new) and float tag (old) produce identical bytes."""

    IDS = (ID_A, ID_B, ID_C, *ISSUE_EXAMPLES)

    @unittest.skipIf(Point2 is None, 'influxdb-client not installed')
    def test_influxdb2_line_protocol(self):
        for api_id in self.IDS:
            old = Point2.from_dict(record(float(api_id)), write_precision=WP2.NS)
            new = Point2.from_dict(record(spd.sensor_id_tag(api_id)),
                                   write_precision=WP2.NS)
            self.assertEqual(new.to_line_protocol(), old.to_line_protocol())

    @unittest.skipIf(Point3 is None, 'influxdb3-python not installed')
    def test_influxdb3_line_protocol(self):
        for api_id in self.IDS:
            old = Point3.from_dict(record(float(api_id)), write_precision=WP3.NS)
            new = Point3.from_dict(record(spd.sensor_id_tag(api_id)),
                                   write_precision=WP3.NS)
            self.assertEqual(new.to_line_protocol(), old.to_line_protocol())

    def test_victoriametrics_json(self):
        w = spd.VMWriter({'url': 'http://vm.invalid', 'verify_ssl': True,
                          'measurement_name': 'SensorPush'})
        for api_id in self.IDS:
            old, new = record(float(api_id)), record(spd.sensor_id_tag(api_id))
            self.assertEqual(
                w._to_json_lines(new['measurement'], new['tags'], new['fields'], new['time']),
                w._to_json_lines(old['measurement'], old['tags'], old['fields'], old['time']))

    def test_record_builders_store_the_tag(self):
        sensors = {ID_A: {'id': ID_A, 'name': 'Kitchen', 'battery_voltage': 2.9,
                          'rssi': -70}}
        samples = {'sensors': {ID_A: [{'observed': '2026-11-08T11:00:00.000Z',
                                       'temperature': 70.0, 'humidity': 45.0}]}}
        import datetime
        volt = spd.build_voltage_records(
            sensors, 'SensorPush',
            datetime.datetime(2026, 11, 8, tzinfo=datetime.timezone.utc))
        recs = spd.process_samples(samples, sensors, 'SensorPush', 0.0, False)
        for r in volt + recs:
            self.assertEqual(r['tags']['sensor_id'], str(float(ID_A)))
            self.assertNotIn('sensor_uid', r['tags'])
        json.dumps(volt + recs)  # still plain data


if __name__ == '__main__':
    unittest.main()
