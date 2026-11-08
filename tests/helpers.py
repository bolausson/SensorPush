"""Shared helpers for the offline test suite.

Run from the repository root:

    python3 -m unittest discover -s tests -v

Nothing here touches the network, the SensorPush API, a real database or
the daemon's real state directory: every test daemon gets its own
temporary STATE_DIRECTORY, and outbound connections raise.
"""
import argparse
import datetime
import importlib.util
import os
import socket
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
UTC = datetime.timezone.utc

_MODULES = {}


def load_script(filename, name):
    """Import one of the repository's scripts as a module (once)."""
    if name not in _MODULES:
        spec = importlib.util.spec_from_file_location(name, REPO / filename)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _MODULES[name] = mod
    return _MODULES[name]


def sensorpushd():
    return load_script('sensorpushd.py', 'sensorpushd_under_test')


def _no_network(*_a, **_k):
    raise OSError('network access is disabled in the test suite')


socket.socket.connect = _no_network
socket.socket.connect_ex = _no_network
socket.create_connection = _no_network


# Synthetic ids of the real shapes: <deviceId>.<16 digits>, with 5-, 6- and
# 8-digit deviceIds.  (Never commit real account ids.)
ID_A = '12345.6789012345678901'
ID_B = '234567.8901234567890123'
ID_C = '16777301.2345678901234567'


def utc(*a):
    return datetime.datetime(*a, tzinfo=UTC)


def make_args(**kw):
    args = dict(daemon=True, timestep=720, qlimit=0, delay=0, dryrun=False,
                verbose=False, sensorlist=None, noconvert=False, interval=None,
                starttime='', stoptime='', backlog='1d')
    args.update(kw)
    return argparse.Namespace(**args)


def make_config(**daemon):
    spd = sensorpushd()
    d = dict(spd.DEFAULT_DAEMON_CFG)
    d.update(daemon)
    return {'login': 'x', 'password': 'x', 'backend': 'influxdb2',
            'my_altitude': 0.0, 'force_ipv4': False,
            'influxdb2': None, 'influxdb3': None, 'victoriametrics': None,
            'daemon': d}


def sensors(*ids):
    return {sid: {'id': sid, 'name': f'S{i}', 'battery_voltage': 3.0,
                  'rssi': -60}
            for i, sid in enumerate(ids)}


class FakeAPI:
    """Stands in for SensorPushAPI; one sample per call window per sensor."""

    def __init__(self, sensor_ids, observed=None):
        self.sensor_ids = list(sensor_ids)
        self.observed = observed
        self.calls = []

    def _ensure_auth(self):
        pass

    def get_sensors(self):
        return sensors(*self.sensor_ids)

    def get_reports(self):
        return {}

    def get_samples(self, start, stop, measures=None, limit=0, sensors=None):
        self.calls.append((start, stop, tuple(sensors or ())))
        ids = [s for s in self.sensor_ids if not sensors or s in sensors]
        observed = self.observed or stop
        data = {sid: [{'observed': observed, 'temperature': 68.0,
                       'humidity': 40.0}] for sid in ids}
        return {'sensors': data, 'truncated': False,
                'total_samples': len(ids), 'total_sensors': len(ids)}


_WRITER_CLASS = []
_WRITER_COUNT = [0]


def fake_writer(name='SensorPush', newest=None, fail=(), sensor_uid=False):
    """An in-memory backend implementing the BaseWriter contract.

    ``newest`` maps a stored sensor_id to its newest timestamp; ``fail`` is
    a set of stored sensor_ids whose lookup raises, or True for all.  Each
    call returns its own class (FakeWriter1, FakeWriter2, ...), because the
    daemon tells backends apart by class name, as real ones are.
    """
    if not _WRITER_CLASS:
        spd = sensorpushd()

        class FakeWriter(spd.BaseWriter):
            def connect(self):
                self._connected = True

            def write(self, records):
                self.written.append(records)

            def get_last_timestamp(self, measurement_name, sensor_id=None,
                                   lookback_min=0):
                self.lookups.append((measurement_name, sensor_id))
                if self.fail is True or sensor_id in self.fail:
                    raise TimeoutError('simulated lookup timeout')
                return self.newest.get(sensor_id)

            def close(self):
                self._connected = False

        _WRITER_CLASS.append(FakeWriter)
    _WRITER_COUNT[0] += 1
    cls = type(f'FakeWriter{_WRITER_COUNT[0]}', (_WRITER_CLASS[0],), {})
    w = cls()
    w.measurement_name = name
    w.newest = dict(newest or {})
    w.fail = fail
    w.sensor_uid = sensor_uid
    w.lookups = []
    w.written = []
    w.connect()
    return w


class DaemonTestCase(unittest.TestCase):
    """Gives each test a private state directory and a daemon factory."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self._tmp.name)
        self._env = {k: os.environ.get(k) for k in
                     ('STATE_DIRECTORY', 'XDG_STATE_HOME', 'NOTIFY_SOCKET',
                      'WATCHDOG_USEC')}
        os.environ['STATE_DIRECTORY'] = str(self.state_dir)
        os.environ['XDG_STATE_HOME'] = str(self.state_dir / 'xdg')
        os.environ.pop('NOTIFY_SOCKET', None)
        os.environ.pop('WATCHDOG_USEC', None)

    def tearDown(self):
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self._tmp.cleanup()

    def make_daemon(self, writers, api=None, args=None, **daemon_cfg):
        spd = sensorpushd()
        d = spd.SensorPushDaemon(api or FakeAPI([ID_A]), writers,
                                 make_config(**daemon_cfg),
                                 args or make_args())
        state = Path(d._state_path).resolve()
        assert str(state).startswith(str(self.state_dir.resolve())), state
        d._interruptible_sleep = lambda _s: None
        return d
