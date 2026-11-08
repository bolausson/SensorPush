"""Config loading: the template's defaults and the SENSOR_UID option."""
import configparser
import tempfile
import textwrap
import unittest
from pathlib import Path

from helpers import sensorpushd

spd = sensorpushd()

# What the previous (ConfigParser-written) template produced.  The text
# template must parse to exactly these values, plus SENSOR_UID = False.
OLD_DEFAULTS = {
    'login': 'SensorPush login (email)', 'password': 'SensorPush password',
    'backend': 'victoriametrics', 'my_altitude': 0.0, 'force_ipv4': False,
    'influxdb2': {'measurement_name': 'SensorPush', 'url': 'http://localhost:8086',
                  'token': 'your_influxdb2_token', 'org': 'your_org',
                  'bucket': 'sensorpush', 'verify_ssl': False},
    'influxdb3': {'measurement_name': 'SensorPush', 'url': 'http://localhost:8181',
                  'database': 'sensorpush', 'token': 'your_influxdb3_token',
                  'verify_ssl': False},
    'victoriametrics': {'measurement_name': 'SensorPush',
                        'url': 'http://localhost:8428', 'verify_ssl': False},
    'daemon': {'interval': 300, 'poll_backlog': '10m', 'max_backfill': '0',
               'deep_scan_interval': '1h', 'max_consecutive_failures': 0,
               'sensor_return_grace': '1h', 'gap_lookback': '0',
               'sensor_stale_after': '6h'},
}


def load(text):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'test.conf'
        path.write_text(textwrap.dedent(text))
        return spd.load_config(str(path))


class Template(unittest.TestCase):

    def test_generated_template_keeps_every_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'sensorpushd.conf'
            spd.create_default_config(str(path))
            cfg = spd.load_config(str(path))
        expected = {k: (dict(v) if isinstance(v, dict) else v)
                    for k, v in OLD_DEFAULTS.items()}
        for backend in ('influxdb2', 'influxdb3', 'victoriametrics'):
            expected[backend]['sensor_uid'] = False
        # The one intended change: 10m polls silently lost most samples.
        expected['daemon']['poll_backlog'] = '1h'
        self.assertEqual(cfg, expected)
        self.assertEqual(cfg['daemon'], dict(spd.DEFAULT_DAEMON_CFG))

    def test_template_comments_are_real_comments(self):
        parser = configparser.ConfigParser()
        parser.read_string(spd.DEFAULT_CONFIG_TEMPLATE)
        for section in parser.sections():
            for key, value in parser[section].items():
                self.assertNotIn('#', value, f'[{section}] {key}')


class SensorUidOption(unittest.TestCase):

    def test_unified_per_backend(self):
        cfg = load("""
            [SENSORPUSHAPI]
            LOGIN = a
            PASSWORD = b
            [BACKEND]
            TYPE = influxdb2, victoriametrics
            [INFLUXDB2]
            TOKEN = t
            [VICTORIAMETRICS]
            SENSOR_UID = yes
            """)
        self.assertFalse(cfg['influxdb2']['sensor_uid'])
        self.assertTrue(cfg['victoriametrics']['sensor_uid'])

    def test_legacy_influxdb2_config(self):
        legacy = """
            [SONSORPUSHAPI]
            LOGIN = a
            PASSWD = b
            [INFLUXDBCONF]
            IFDB_URL = http://localhost
            IFDB_PORT = 8086
            IFDB_TOKEN = t
            IFDB_ORG = o
            IFDB_BUCKET = b
            {extra}
            [MISC]
            MY_ALTITUDE = 0
            """
        self.assertFalse(load(legacy.format(extra=''))['influxdb2']['sensor_uid'])
        self.assertTrue(load(legacy.format(extra='SENSOR_UID = True'))
                        ['influxdb2']['sensor_uid'])

    def test_legacy_victoriametrics_config(self):
        cfg = load("""
            [SENSORPUSHAPI]
            LOGIN = a
            PASSWD = b
            [VICTORIAMETRICSCONF]
            VM_URL = http://localhost:8428
            SENSOR_UID = 1
            [MISC]
            MY_ALTITUDE = 0
            """)
        self.assertTrue(cfg['victoriametrics']['sensor_uid'])

    def test_typo_is_an_error_not_a_silent_switch(self):
        with self.assertRaises(ValueError):
            load("""
                [SENSORPUSHAPI]
                LOGIN = a
                PASSWORD = b
                [INFLUXDB2]
                SENSOR_UID = yse
                """)

    def test_writers_pick_it_up(self):
        on = spd.create_writer('victoriametrics', {'victoriametrics': {
            'measurement_name': 'SensorPush', 'url': 'http://vm.invalid',
            'verify_ssl': True, 'sensor_uid': True}})
        off = spd.create_writer('victoriametrics', {'victoriametrics': {
            'measurement_name': 'SensorPush', 'url': 'http://vm.invalid',
            'verify_ssl': True}})
        self.assertTrue(on.sensor_uid)
        self.assertFalse(off.sensor_uid)


if __name__ == '__main__':
    unittest.main()
