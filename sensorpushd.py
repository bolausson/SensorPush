#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# sensorpushd.py, Copyright Bjoern Olausson
# -----------------------------------------------------------------------------
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# To view the license visit
# http://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# or write to
# Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
# 02110-1301 USA
# -----------------------------------------------------------------------------
#
# Unified SensorPush daemon - queries the SensorPush API and stores
# temperature, humidity, and other sensor readings in InfluxDB 2,
# InfluxDB 3, and/or VictoriaMetrics.  Multiple backends can be
# active simultaneously.
#
# Can run as a one-shot command (backward compatible with cron) or as a
# continuous daemon managed by systemd.
#

import os
import sys
import json
import time
import math
import signal
import socket
import logging
import requests
import datetime
import argparse
import configparser
from abc import ABC, abstractmethod
from pathlib import Path
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings globally
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def sd_notify(state):
    """Send a notification to the systemd watchdog (if available).

    Uses the NOTIFY_SOCKET environment variable set by systemd for
    Type=notify or WatchdogSec services.  Silently does nothing when
    the socket is not available (e.g. running outside systemd).
    """
    sock_path = os.environ.get('NOTIFY_SOCKET')
    if not sock_path:
        return
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if sock_path.startswith('@'):
            sock_path = '\0' + sock_path[1:]
        sock.sendto(state.encode(), sock_path)
        sock.close()
    except Exception:
        pass

logger = logging.getLogger('sensorpushd')

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
VERIFY_SSL = False
RETRYWAIT = 60
MAXRETRY = 3
RETRY_DELAYS = [10, 30, 60, 120, 300]

API_URL_BASE = 'https://api.sensorpush.com/api/v1'
API_URL_OA_AUTH = f'{API_URL_BASE}/oauth/authorize'
API_URL_OA_ATOK = f'{API_URL_BASE}/oauth/accesstoken'
API_URL_GW = f'{API_URL_BASE}/devices/gateways'
API_URL_SE = f'{API_URL_BASE}/devices/sensors'
API_URL_SPL = f'{API_URL_BASE}/samples'
API_URL_RPL = f'{API_URL_BASE}/reports/list'
API_URL_RPDL = f'{API_URL_BASE}/reports/download'

HTTP_OA_HEAD = {'accept': 'application/json',
                'Content-Type': 'application/json'}

MEASURES = ["altitude", "barometric_pressure", "dewpoint", "humidity",
            "temperature", "vpd", "distance"]


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------
def setup_logging(level='INFO', log_file=None):
    fmt = '%(asctime)s %(name)s %(levelname)s %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    handlers = []
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))
    logging.basicConfig(level=getattr(logging, level),
                        format=fmt, datefmt=datefmt,
                        handlers=handlers)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
def load_config(config_path):
    """Load config from file. Auto-detects legacy formats."""
    config = configparser.ConfigParser()

    if not Path(config_path).is_file():
        create_default_config(config_path)
        print(f'Created config file template at {config_path}')
        print('Please edit the config file with your settings and run again.')
        sys.exit(0)

    config.read(config_path)

    # Detect legacy InfluxDB 2 format (~/.sensorpush.conf)
    if 'INFLUXDBCONF' in config:
        return _load_legacy_influxdb2(config)

    # Detect legacy VictoriaMetrics format (~/.sensorpush_vm.conf)
    if 'VICTORIAMETRICSCONF' in config:
        return _load_legacy_vm(config)

    # New unified format (~/.sensorpushd.conf)
    return _load_unified(config)


def _load_legacy_influxdb2(config):
    """Load legacy ~/.sensorpush.conf format."""
    # Note: original config uses 'SONSORPUSHAPI' (typo preserved for compat)
    section = 'SONSORPUSHAPI' if 'SONSORPUSHAPI' in config else 'SENSORPUSHAPI'
    # Legacy config has separate IFDB_URL and IFDB_PORT — combine into one URL
    url = config['INFLUXDBCONF']['IFDB_URL']
    port = config['INFLUXDBCONF']['IFDB_PORT']
    host_part = url.split('://', 1)[-1]
    if ':' not in host_part:
        url = f'{url}:{port}'
    return {
        'login': config[section]['LOGIN'],
        'password': config[section]['PASSWD'],
        'backend': 'influxdb2',
        'my_altitude': float(config['MISC'].get('MY_ALTITUDE', '0')),
        'force_ipv4': config['MISC'].get('FORCE_IPv4', 'False').lower() in ('true', '1', 'yes'),
        'influxdb2': {
            'measurement_name': config['INFLUXDBCONF'].get('MEASUREMENT_NAME', 'SensorPush'),
            'url': url,
            'token': config['INFLUXDBCONF']['IFDB_TOKEN'],
            'org': config['INFLUXDBCONF']['IFDB_ORG'],
            'bucket': config['INFLUXDBCONF']['IFDB_BUCKET'],
            'verify_ssl': config['INFLUXDBCONF'].get('IFDB_VERIFY_SSL', 'False').lower() in ('true', '1', 'yes'),
        },
        'influxdb3': None,
        'victoriametrics': None,
        'daemon': {
            'interval': 300,
            'poll_backlog': '10m',
        },
    }


def _load_legacy_vm(config):
    """Load legacy ~/.sensorpush_vm.conf format."""
    section = 'SONSORPUSHAPI' if 'SONSORPUSHAPI' in config else 'SENSORPUSHAPI'
    return {
        'login': config[section]['LOGIN'],
        'password': config[section]['PASSWD'],
        'backend': 'victoriametrics',
        'my_altitude': float(config['MISC'].get('MY_ALTITUDE', '0')),
        'force_ipv4': config['MISC'].getboolean('FORCE_IPv4', False),
        'influxdb2': None,
        'influxdb3': None,
        'victoriametrics': {
            'measurement_name': config['VICTORIAMETRICSCONF'].get('MEASUREMENT_NAME', 'SensorPush'),
            'url': config['VICTORIAMETRICSCONF'].get('VM_URL', 'http://localhost:8428'),
            'verify_ssl': config['VICTORIAMETRICSCONF'].getboolean('VM_VERIFY_SSL', False),
        },
        'daemon': {
            'interval': 300,
            'poll_backlog': '10m',
        },
    }


def _load_unified(config):
    """Load new unified ~/.sensorpushd.conf format."""
    section = 'SONSORPUSHAPI' if 'SONSORPUSHAPI' in config else 'SENSORPUSHAPI'

    # Accept both 'PASSWD' (legacy) and 'PASSWORD' (new) key names
    if config.has_option(section, 'PASSWD'):
        password = config[section]['PASSWD']
    else:
        password = config[section]['PASSWORD']

    cfg = {
        'login': config.get(section, 'LOGIN', fallback=config.get(section, 'EMAIL', fallback='')),
        'password': password,
        'backend': config.get('BACKEND', 'TYPE', fallback='victoriametrics'),
        'my_altitude': config.getfloat('MISC', 'MY_ALTITUDE', fallback=0.0),
        'force_ipv4': config.getboolean('MISC', 'FORCE_IPv4', fallback=False),
        'influxdb2': None,
        'influxdb3': None,
        'victoriametrics': None,
        'daemon': {
            'interval': config.getint('DAEMON', 'INTERVAL', fallback=300),
            'poll_backlog': config.get('DAEMON', 'POLL_BACKLOG', fallback='10m'),
        },
    }

    if 'INFLUXDB2' in config:
        url = config.get('INFLUXDB2', 'URL', fallback='http://localhost:8086')
        # Legacy compat: if PORT is set and URL doesn't already contain a port
        if config.has_option('INFLUXDB2', 'PORT'):
            port = config.get('INFLUXDB2', 'PORT')
            host_part = url.split('://', 1)[-1]
            if ':' not in host_part:
                url = f'{url}:{port}'
        cfg['influxdb2'] = {
            'measurement_name': config.get('INFLUXDB2', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': url,
            'token': config.get('INFLUXDB2', 'TOKEN', fallback=''),
            'org': config.get('INFLUXDB2', 'ORG', fallback=''),
            'bucket': config.get('INFLUXDB2', 'BUCKET', fallback='sensorpush'),
            'verify_ssl': config.getboolean('INFLUXDB2', 'VERIFY_SSL', fallback=False),
        }

    if 'INFLUXDB3' in config:
        # Read URL (new) or HOST (legacy) for backward compat
        if config.has_option('INFLUXDB3', 'URL'):
            url = config.get('INFLUXDB3', 'URL')
        else:
            url = config.get('INFLUXDB3', 'HOST', fallback='http://localhost:8181')
        cfg['influxdb3'] = {
            'measurement_name': config.get('INFLUXDB3', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': url,
            'database': config.get('INFLUXDB3', 'DATABASE', fallback='sensorpush'),
            'token': config.get('INFLUXDB3', 'TOKEN', fallback=''),
            'verify_ssl': config.getboolean('INFLUXDB3', 'VERIFY_SSL', fallback=False),
        }

    if 'VICTORIAMETRICS' in config:
        cfg['victoriametrics'] = {
            'measurement_name': config.get('VICTORIAMETRICS', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': config.get('VICTORIAMETRICS', 'URL', fallback='http://localhost:8428'),
            'verify_ssl': config.getboolean('VICTORIAMETRICS', 'VERIFY_SSL', fallback=False),
        }

    return cfg


def create_default_config(path):
    """Write a default config template."""
    config = configparser.ConfigParser()
    config['SENSORPUSHAPI'] = {
        'LOGIN': 'SensorPush login (email)',
        'PASSWORD': 'SensorPush password',
    }
    config['BACKEND'] = {
        'TYPE': 'victoriametrics',
    }
    config['INFLUXDB2'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'URL': 'http://localhost:8086',
        'TOKEN': 'your_influxdb2_token',
        'ORG': 'your_org',
        'BUCKET': 'sensorpush',
        'VERIFY_SSL': 'False',
    }
    config['INFLUXDB3'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'URL': 'http://localhost:8181',
        'DATABASE': 'sensorpush',
        'TOKEN': 'your_influxdb3_token',
        'VERIFY_SSL': 'False',
    }
    config['VICTORIAMETRICS'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'URL': 'http://localhost:8428',
        'VERIFY_SSL': 'False',
    }
    config['DAEMON'] = {
        'INTERVAL': '300',
        'POLL_BACKLOG': '10m',
    }
    config['MISC'] = {
        'MY_ALTITUDE': '0.0',
        'FORCE_IPv4': 'False',
    }
    with open(path, 'w') as f:
        config.write(f)


# -----------------------------------------------------------------------------
# CLI argument parsing
# -----------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description='Unified SensorPush daemon - queries the SensorPush API and '
                    'stores readings in InfluxDB 2, InfluxDB 3, and/or VictoriaMetrics')
    parser.add_argument(
        '-s', '--start', dest='starttime', default='', type=str,
        help='Start query at time (e.g. "2019-07-25T00:10:41+0200")')
    parser.add_argument(
        '-p', '--stop', dest='stoptime', default='', type=str,
        help='Stop query at time (e.g. "2019-07-26T00:10:41+0200")')
    parser.add_argument(
        '-b', '--backlog', dest='backlog', default='1d', type=str,
        help='Historical data to fetch (default 1d) - format: <number>[m|h|d|w|M|Y]')
    parser.add_argument(
        '-t', '--timestep', dest='timestep', default=720, type=int,
        help='Time slice per query in minutes (default 720 = 12h)')
    parser.add_argument(
        '-q', '--querylimit', dest='qlimit', default=0, type=int,
        help='Max samples per sensor (0 = unlimited, default 0)')
    parser.add_argument(
        '-d', '--delay', dest='delay', default=60, type=int,
        help='Delay in seconds between queries (default 60)')
    parser.add_argument(
        '-l', '--listsensors', dest='listsensors', action='store_true',
        help='Show a list of sensors and exit')
    parser.add_argument(
        '-g', '--listgateways', dest='listgateways', action='store_true',
        help='Show a list of gateways and exit')
    parser.add_argument(
        '-i', '--sensorlist', dest='sensorlist', nargs='+', type=str,
        help='List of sensor IDs to query')
    parser.add_argument(
        '-n', '--noconvert', dest='noconvert', action='store_true',
        help='Do not convert F to C, inHG to mBar, kPa to mBar and feet to meters')
    parser.add_argument(
        '-x', '--dryrun', dest='dryrun', action='store_true',
        help='Do not write anything to the database, just print what would have been written')
    parser.add_argument(
        '-v', '--verbose', dest='verbose', action='store_true',
        help='Show full output in dryrun mode (do not truncate)')
    # New arguments
    parser.add_argument(
        '--backend', dest='backend',
        nargs='+',
        choices=['influxdb2', 'influxdb3', 'victoriametrics'],
        default=None,
        help='Database backend(s) (overrides config file). '
             'Specify multiple to write to all simultaneously')
    parser.add_argument(
        '--daemon', dest='daemon', action='store_true', default=False,
        help='Run as a continuous daemon (default: one-shot mode)')
    parser.add_argument(
        '--interval', dest='interval', default=None, type=int,
        help='Polling interval in seconds for daemon mode (default: from config or 300)')
    parser.add_argument(
        '-c', '--config', dest='config', default=None, type=str,
        help='Path to config file (default: ~/.sensorpushd.conf)')
    parser.add_argument(
        '--log-level', dest='loglevel',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging verbosity (default: INFO)')
    parser.add_argument(
        '--log-file', dest='logfile', default=None, type=str,
        help='Log to file instead of stderr')
    parser.add_argument(
        '--generate-config', dest='generate_config', nargs='?',
        const='', default=None, metavar='PATH',
        help='Generate a default config file and exit. '
             'Optionally specify output path (default: ~/.sensorpushd.conf)')
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------
def local_time_offset(t=None):
    """Return offset of local zone from GMT, either at present or at time t."""
    if t is None:
        t = time.time()
    if time.localtime(t).tm_isdst and time.daylight:
        return -time.altzone / 3600
    else:
        return -time.timezone / 3600


def F_to_C(F, noconvert=False):
    if noconvert:
        return float(F)
    try:
        return float(round((F - 32) * 5.0 / 9.0, 2))
    except TypeError:
        return 0.0


def ft_to_m(ft, noconvert=False):
    if noconvert:
        return float(ft)
    return float(round(ft * 0.3048, 2))


def inHg_to_mBar(inHg, noconvert=False):
    if noconvert:
        return float(inHg)
    return float(round(inHg * 33.8639, 2))


def kPa_to_mBar(kPa, noconvert=False):
    if noconvert:
        return float(kPa)
    return float(round(kPa * 10, 2))


def parse_backlog(backlogstring):
    """Convert backlog string like '1d', '10m', '1M' to minutes."""
    minutes_per_unit = {
        "m": 1, "h": 60, "d": 60 * 24, "w": 60 * 24 * 7,
        "M": 60 * 24 * 30.417, "Y": 60 * 24 * 365
    }
    return int(int(backlogstring[:-1]) * minutes_per_unit[backlogstring[-1]])


def build_timelist(starttime, stoptime, timesteps):
    """Build list of [start, stop] time windows with 30-min overlaps."""
    timelist = []
    newstartt = None
    while starttime <= stoptime:
        start = datetime.date.strftime(starttime, '%Y-%m-%dT%X%z')
        if newstartt is not None:
            nextstop = newstartt + datetime.timedelta(minutes=int(timesteps))
        else:
            nextstop = starttime + datetime.timedelta(minutes=int(timesteps))
        stop = datetime.date.strftime(nextstop, '%Y-%m-%dT%X%z')
        starttime = nextstop - datetime.timedelta(minutes=30)
        newstartt = nextstop
        timelist.append([start, stop])
    return timelist


def parse_timestamp_to_ms(timestamp):
    """Convert timestamp string to milliseconds for VictoriaMetrics."""
    if isinstance(timestamp, str):
        try:
            dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            dt = datetime.datetime.strptime(timestamp.split('.')[0], '%Y-%m-%dT%H:%M:%S')
        return int(dt.timestamp() * 1000)
    return int(timestamp * 1000)


# -----------------------------------------------------------------------------
# Backend Writers
# -----------------------------------------------------------------------------
class BaseWriter(ABC):
    """Abstract base class for database backend writers."""

    def __init__(self):
        self._connected = False
        self._consecutive_failures = 0

    @property
    def connected(self):
        return self._connected

    @abstractmethod
    def connect(self):
        """Establish connection to the backend."""
        pass

    @abstractmethod
    def write(self, records):
        """Write a list of measurement dicts to the backend.

        Each record is: {'measurement': str, 'tags': dict, 'fields': dict, 'time': str}
        """
        pass

    @abstractmethod
    def get_last_timestamp(self, measurement_name, sensor_id=None):
        """Query the backend for the most recent data point timestamp.

        If sensor_id is given, returns the last timestamp for that
        specific sensor (using the temperature metric as reference).
        Returns a datetime object or None if no data exists.
        """
        pass

    @abstractmethod
    def close(self):
        """Close the backend connection."""
        pass

    def reconnect(self):
        """Tear down and re-establish the connection."""
        try:
            self.close()
        except Exception:
            pass
        self._connected = False
        self.connect()


class InfluxDB2Writer(BaseWriter):
    """InfluxDB 2.x backend writer."""

    def __init__(self, config):
        super().__init__()
        self.url = config['url']
        self.token = config['token']
        self.org = config['org']
        self.bucket = config['bucket']
        self.verify_ssl = config['verify_ssl']
        self.measurement_name = config['measurement_name']
        self.client = None
        self.write_api = None
        self.query_api = None

    def connect(self):
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS
        except ImportError:
            raise ImportError(
                "InfluxDB 2 backend requires the 'influxdb-client' package. "
                "Install with: pip install influxdb-client"
            )
        self.client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            verify_ssl=self.verify_ssl
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self._connected = True
        logger.info("Connected to InfluxDB 2 at %s", self.url)

    def write(self, records):
        if not records:
            return
        self.write_api.write(bucket=self.bucket, org=self.org, record=records)

    def get_last_timestamp(self, measurement_name, sensor_id=None):
        try:
            sensor_filter = ''
            if sensor_id is not None:
                sensor_filter = (
                    f' |> filter(fn: (r) => r.sensor_id == "{sensor_id}")'
                )
            query = (
                f'from(bucket: "{self.bucket}")'
                f' |> range(start: -30d)'
                f' |> filter(fn: (r) => r._measurement == "{measurement_name}"'
                f'    and r._field == "temperature")'
                f'{sensor_filter}'
                f' |> last()'
            )
            tables = self.query_api.query(query, org=self.org)
            for table in tables:
                for record in table.records:
                    return record.get_time()
        except Exception as e:
            logger.warning("Failed to query last timestamp from InfluxDB 2: %s", e)
        return None

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self.write_api = None
            self.query_api = None
            self._connected = False
            logger.info("InfluxDB 2 connection closed")


class InfluxDB3Writer(BaseWriter):
    """InfluxDB 3.x backend writer."""

    def __init__(self, config):
        super().__init__()
        self.url = config['url']
        self.database = config['database']
        self.token = config['token']
        self.verify_ssl = config.get('verify_ssl', False)
        self.measurement_name = config['measurement_name']
        self.client = None

    def connect(self):
        try:
            from influxdb_client_3 import InfluxDBClient3, write_client_options, SYNCHRONOUS
        except ImportError:
            raise ImportError(
                "InfluxDB 3 backend requires the 'influxdb3-python' package. "
                "Install with: pip install influxdb3-python"
            )
        wco = write_client_options(write_options=SYNCHRONOUS)
        self.client = InfluxDBClient3(
            host=self.url,
            database=self.database,
            token=self.token,
            write_client_options=wco
        )
        self._connected = True
        logger.info("Connected to InfluxDB 3 at %s", self.url)

    def write(self, records):
        if not records:
            return
        self.client.write(record=records)

    def get_last_timestamp(self, measurement_name, sensor_id=None):
        try:
            sensor_filter = ''
            if sensor_id is not None:
                sensor_filter = f" AND sensor_id = '{sensor_id}'"
            query = (
                f'SELECT max(time) AS last_time FROM "{measurement_name}"'
                f" WHERE time > now() - INTERVAL '30 days'"
                f'{sensor_filter}'
            )
            result = self.client.query(query)
            if result and len(result) > 0:
                last_time = result.column('last_time')[0].as_py()
                if last_time:
                    return last_time
        except Exception as e:
            logger.warning("Failed to query last timestamp from InfluxDB 3: %s", e)
        return None

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self._connected = False
            logger.info("InfluxDB 3 connection closed")


class VMWriter(BaseWriter):
    """VictoriaMetrics backend writer using native JSON import API."""

    def __init__(self, config):
        super().__init__()
        self.url = config['url']
        self.verify_ssl = config['verify_ssl']
        self.measurement_name = config['measurement_name']
        self.session = None

    def connect(self):
        self.session = requests.Session()
        self._connected = True
        logger.info("VictoriaMetrics writer ready for %s", self.url)

    def write(self, records):
        if not records:
            return
        lines = []
        for record in records:
            lines.extend(self._to_json_lines(
                record['measurement'], record['tags'],
                record['fields'], record['time']
            ))
        if not lines:
            return
        data = '\n'.join(lines)
        url = f'{self.url}/api/v1/import'
        response = self.session.post(
            url,
            data=data.encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            verify=self.verify_ssl,
            timeout=15
        )
        response.raise_for_status()

    def _to_json_lines(self, measurement_name, tags, fields, timestamp):
        """Convert to VictoriaMetrics native JSON format."""
        timestamp_ms = parse_timestamp_to_ms(timestamp)
        lines = []
        for field_name, field_value in fields.items():
            metric = {'__name__': f'{measurement_name}_{field_name}'}
            for tag_key, tag_value in tags.items():
                metric[tag_key] = str(tag_value)
            json_obj = {
                'metric': metric,
                'values': [float(field_value)],
                'timestamps': [timestamp_ms]
            }
            lines.append(json.dumps(json_obj, ensure_ascii=False))
        return lines

    def get_last_timestamp(self, measurement_name, sensor_id=None):
        try:
            # Use last_over_time() which is standard MetricsQL.  The response
            # timestamp in the [value] array tells us when the last point was.
            sensor_filter = ''
            if sensor_id is not None:
                sensor_filter = f'sensor_id="{sensor_id}"'
            url = f'{self.url}/api/v1/query'
            metric = f'{measurement_name}_temperature'
            query_str = f'last_over_time({metric}{{{sensor_filter}}}[30d])'
            response = self.session.get(
                url,
                params={'query': query_str},
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            results = data.get('data', {}).get('result', [])
            if results:
                # [0] = unix timestamp, [1] = value
                ts = float(results[0]['value'][0])
                return datetime.datetime.fromtimestamp(
                    ts, tz=datetime.timezone.utc)
            return None
        except Exception as e:
            logger.warning("Failed to query last timestamp from VictoriaMetrics: %s", e)
        return None

    def close(self):
        if self.session:
            self.session.close()
            self.session = None
            self._connected = False
            logger.info("VictoriaMetrics session closed")


def create_writer(backend_name, config):
    """Factory: create the appropriate backend writer."""
    if backend_name == 'influxdb2':
        if not config.get('influxdb2'):
            raise ValueError(
                "InfluxDB 2 backend selected but [INFLUXDB2] config section is missing")
        return InfluxDB2Writer(config['influxdb2'])
    elif backend_name == 'influxdb3':
        if not config.get('influxdb3'):
            raise ValueError(
                "InfluxDB 3 backend selected but [INFLUXDB3] config section is missing")
        return InfluxDB3Writer(config['influxdb3'])
    elif backend_name == 'victoriametrics':
        if not config.get('victoriametrics'):
            raise ValueError(
                "VictoriaMetrics backend selected but [VICTORIAMETRICS] config section is missing")
        return VMWriter(config['victoriametrics'])
    else:
        raise ValueError(f"Unknown backend: {backend_name}")


def create_writers(backend_names, config):
    """Create writer instances for all specified backends."""
    writers = []
    for name in backend_names:
        writers.append(create_writer(name, config))
    return writers


# -----------------------------------------------------------------------------
# SensorPush API
# -----------------------------------------------------------------------------
class SensorPushAPI:
    """Client for the SensorPush cloud API."""

    def __init__(self, login, password, verify_ssl=False, force_ipv4=False):
        self.login = login
        self.password = password
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.mount(API_URL_BASE, HTTPAdapter(max_retries=10))
        self.access_token = None
        self._token_time = None
        self._auth_header = None

    def authenticate(self):
        """Perform OAuth authentication flow with retry."""
        http_data = json.dumps({'email': self.login, 'password': self.password})

        # Step 1: Get authorization string
        auth = None
        for attempt in range(1, MAXRETRY + 1):
            logger.info("Fetching API oauth authorization string - try %d/%d",
                        attempt, MAXRETRY)
            try:
                r = self.session.post(API_URL_OA_AUTH, headers=HTTP_OA_HEAD,
                                      data=http_data, verify=self.verify_ssl)
                if r.status_code == 200:
                    auth = r.content.decode('utf-8')
                    break
                else:
                    logger.error("Auth request failed with status %d", r.status_code)
            except requests.exceptions.ConnectionError as e:
                logger.warning("Connection error during auth: %s", e)

            if attempt >= MAXRETRY:
                raise ConnectionError(
                    f"Failed to fetch API oauth authorization after {MAXRETRY} attempts")
            time.sleep(20)

        # Step 2: Get access token
        logger.info("Fetching API oauth access token")
        r = self.session.post(API_URL_OA_ATOK, headers=HTTP_OA_HEAD,
                              data=auth, verify=self.verify_ssl)

        if r.status_code == 200:
            self.access_token = json.loads(r.content.decode('utf-8'))['accesstoken']
            self._token_time = time.time()
            self._auth_header = {
                'accept': 'application/json',
                'Authorization': self.access_token
            }
            logger.info("Authentication successful")
        else:
            raise ConnectionError(
                f"Access token request failed with status {r.status_code}")

    def _ensure_auth(self):
        """Re-authenticate if token is likely expired (>55 min)."""
        if (self._token_time is None or
                time.time() - self._token_time > 3300):
            self.authenticate()

    def _post(self, url, data=None):
        """Make an authenticated POST request."""
        self._ensure_auth()
        http_data = json.dumps(data or {})
        r = self.session.post(url, headers=self._auth_header,
                              data=http_data, verify=self.verify_ssl)
        if r.status_code == 200:
            return json.loads(r.content.decode('utf-8'))
        raise ValueError(
            f"API request to {url} failed with status {r.status_code}: "
            f"{r.content.decode('utf-8')}")

    def get_gateways(self):
        logger.info("Fetching the list of gateways")
        return self._post(API_URL_GW)

    def get_sensors(self):
        logger.info("Fetching the list of sensors")
        return self._post(API_URL_SE)

    def get_reports(self):
        logger.info("Fetching the list of bulk reports")
        return self._post(API_URL_RPL)

    def get_samples(self, start, stop, measures=None, limit=0, sensors=None):
        query = {'startTime': start, 'stopTime': stop,
                 'measures': measures or MEASURES}
        if limit != 0:
            query['limit'] = limit
        if sensors:
            query['sensors'] = sensors
        return self._post(API_URL_SPL, query)


# -----------------------------------------------------------------------------
# Data processing
# -----------------------------------------------------------------------------
def build_voltage_records(sensors, measurement_name, querytime):
    """Build voltage/RSSI measurement records for all sensors."""
    records = []
    measurement_v_name = f'{measurement_name}_V'
    for sid in sensors.keys():
        try:
            bat_volt = float(sensors[sid]["battery_voltage"])
        except (KeyError, TypeError):
            bat_volt = 0.0
        try:
            rssi = float(sensors[sid]["rssi"])
        except (KeyError, TypeError):
            rssi = 0.0

        records.append({
            'measurement': str(measurement_v_name),
            'tags': {
                'sensor_id': float(sensors[sid]["id"]),
                'sensor_name': str(sensors[sid]["name"]),
            },
            'fields': {
                'voltage': float(bat_volt),
                'rssi': float(rssi),
            },
            'time': datetime.date.strftime(querytime, '%Y-%m-%dT%X%z')
        })
    return records


def process_samples(samples, sensors, measurement_name, my_altitude, noconvert):
    """Process API sample response into measurement records."""
    records = []

    for key in samples['sensors'].keys():
        for item in samples['sensors'][key]:
            observed = str(item['observed'])
            sensor_name = str(sensors[key]['name'])

            m = {
                'measurement': str(measurement_name),
                'tags': {
                    'sensor_id': float(key),
                    'sensor_name': str(sensor_name),
                },
                'fields': {},
                'time': str(observed)
            }

            try:
                humidity = float(item['humidity'])
            except KeyError:
                pass
            else:
                m['fields']['humidity'] = float(humidity)

            try:
                temperature = F_to_C(item['temperature'], noconvert)
            except KeyError:
                pass
            else:
                m['fields']['temperature'] = float(temperature)

            try:
                pressure = inHg_to_mBar(item['barometric_pressure'], noconvert)
            except KeyError:
                # Absolute humidity (g/m3) - simplified formula without pressure
                # https://carnotcycle.wordpress.com/2012/08/04/how-to-convert-relative-humidity-to-absolute-humidity/
                abs_humidity = float(round(
                    (6.112 * math.e**((17.67 * temperature) / (temperature + 243.5))
                     * humidity * 2.1674) / (273.15 + temperature), 2))
                m['fields']['abs_humidity'] = float(abs_humidity)
            else:
                m['fields']['pressure'] = float(pressure)
                # Absolute humidity (g/m3) - accurate formula with pressure
                # https://www.loxwiki.eu/display/LOX/Absolute+Luftfeuchtigkeit+berechnen
                abs_humidity = float(round(
                    0.622 * humidity / 100 * (
                        1.01325 * 10.0**(5.426651 - 2005.1 / (temperature + 273.15)
                        + 0.00013869 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        / (temperature + 273.15) * (10.0**(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) - 1.0)
                        - 0.0044 * 10.0**((-0.0057148 * (374.11 - temperature)**1.25)))
                        + (((temperature + 273.15) / 647.3) - 0.422)
                        * (0.577 - ((temperature + 273.15) / 647.3))
                        * math.exp(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0))
                        * 0.00980665)
                    / (pressure / 1000.0 - humidity / 100.0 * (
                        1.01325 * 10.0**(5.426651 - 2005.1 / (temperature + 273.15)
                        + 0.00013869 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        / (temperature + 273.15) * (10.0**(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) - 1.0)
                        - 0.0044 * 10.0**((-0.0057148 * (374.11 - temperature)**1.25)))
                        + (((temperature + 273.15) / 647.3) - 0.422)
                        * (0.577 - ((temperature + 273.15) / 647.3))
                        * math.exp(0.000000000011965
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)
                        * ((temperature + 273.15) * (temperature + 273.15) - 293700.0))
                        * 0.00980665))
                    * pressure / 1000.0 * 100000000.0
                    / ((temperature + 273.15) * 287.1), 2))
                m['fields']['abs_humidity'] = float(abs_humidity)

            try:
                altitude = ft_to_m(item['altitude'], noconvert)
            except KeyError:
                altitude = float(my_altitude)
            finally:
                if altitude == 0:
                    altitude = float(my_altitude)
                m['fields']['altitude'] = float(altitude)

            try:
                distance = ft_to_m(item['distance'], noconvert)
            except KeyError:
                pass
            else:
                m['fields']['distance'] = float(distance)

            try:
                dewpoint = F_to_C(item['dewpoint'], noconvert)
            except KeyError:
                # Dewpoint in degree centigrade
                # https://cals.arizona.edu/azmet/dewpoint.html
                dewpoint = float(round(
                    (237.3 * ((math.log(humidity / 100) +
                    ((17.27 * temperature) / (237.3 + temperature))) / 17.27))
                    / (1 - ((math.log(humidity / 100) +
                    ((17.27 * temperature) / (237.3 + temperature))) / 17.27)), 2))
            finally:
                m['fields']['dewpoint'] = float(dewpoint)

            try:
                vpd = kPa_to_mBar(item['vpd'], noconvert)
            except KeyError:
                # Vapor Pressure Deficit in mBar
                # https://pulsegrow.com/blogs/learn/vpd
                vpd = float(kPa_to_mBar(
                    ((610.78 * math.e**(temperature / (temperature + 238.3) * 17.2694))
                     / 1000) * (1 - humidity / 100), noconvert))
            finally:
                m['fields']['vpd'] = float(vpd)

            records.append(m)

    return records


# -----------------------------------------------------------------------------
# Daemon
# -----------------------------------------------------------------------------
class SensorPushDaemon:
    """Main daemon: handles collection cycles, retries, and gap-filling."""

    def __init__(self, api, writers, config, args):
        self.api = api
        self.writers = writers
        self.config = config
        self.args = args
        self.running = True
        self._last_successful_time = None
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGHUP, self._handle_signal)

    def _handle_signal(self, signum, frame):
        if signum in (signal.SIGTERM, signal.SIGINT):
            logger.info("Received %s, shutting down gracefully...",
                        signal.Signals(signum).name)
            self.running = False
        elif signum == signal.SIGHUP:
            logger.info("Received SIGHUP, will reload configuration on next cycle")

    def _get_measurement_name(self):
        """Get the measurement name from the first active backend."""
        if self.writers:
            return self.writers[0].measurement_name
        return 'SensorPush'

    def _interruptible_sleep(self, seconds):
        """Sleep that can be interrupted by signals.

        Sends a systemd watchdog ping every 60 seconds during sleep
        so the WatchdogSec timer does not expire.
        """
        for i in range(seconds):
            if not self.running:
                return
            if i > 0 and i % 60 == 0:
                sd_notify('WATCHDOG=1')
            time.sleep(1)

    def run(self):
        """Main daemon loop."""
        interval = self.args.interval or self.config['daemon']['interval']
        logger.info("Starting SensorPush daemon (interval=%ds)", interval)

        # Connect to all backends (with retry)
        self._connect_writers()

        sd_notify('READY=1')

        consecutive_failures = 0
        max_consecutive_failures = 50

        while self.running:
            sd_notify('WATCHDOG=1')
            try:
                self._collect_cycle()
                consecutive_failures = 0
                self._last_successful_time = datetime.datetime.now(
                    tz=datetime.timezone.utc)
            except Exception as e:
                consecutive_failures += 1
                logger.error("Collection cycle failed (%d/%d): %s",
                             consecutive_failures, max_consecutive_failures, e)
                if consecutive_failures >= max_consecutive_failures:
                    logger.critical(
                        "Too many consecutive failures (%d), exiting",
                        consecutive_failures)
                    break

            self._interruptible_sleep(interval)

        for w in self.writers:
            try:
                w.close()
            except Exception as e:
                logger.warning("Error closing %s: %s", type(w).__name__, e)
        logger.info("Daemon stopped")

    def _connect_writers(self):
        """Connect all backend writers with retries. Fails only if ALL fail.

        Backends that fail to connect are kept in self.writers with
        _connected=False so that _safe_write() can attempt reconnection
        on every subsequent cycle.
        """
        failed = []
        for writer in self.writers:
            for attempt, delay in enumerate(RETRY_DELAYS, 1):
                try:
                    writer.connect()
                    break
                except Exception as e:
                    logger.error("Failed to connect %s (attempt %d/%d): %s",
                                 type(writer).__name__, attempt,
                                 len(RETRY_DELAYS), e)
                    if attempt < len(RETRY_DELAYS):
                        logger.info("Retrying in %ds...", delay)
                        self._interruptible_sleep(delay)
            if not writer.connected:
                failed.append(type(writer).__name__)
        if len(failed) == len(self.writers):
            raise ConnectionError(
                "Failed to connect to any backend: " + ', '.join(failed))
        if failed:
            logger.warning("Some backends unavailable at startup: %s. "
                           "Will keep retrying on each cycle.",
                           ', '.join(failed))

    def _collect_cycle(self):
        """One collection cycle: auth, fetch, process, write."""
        mytz = datetime.timezone(datetime.timedelta(hours=local_time_offset()))
        currenttime = datetime.datetime.now(tz=mytz)
        measurement_name = self._get_measurement_name()

        # Authenticate (or refresh token)
        self.api.authenticate()

        # Fetch and write voltage/RSSI data
        sensors = self.api.get_sensors()
        logger.info("Building voltage records for %d sensors", len(sensors))
        voltage_records = build_voltage_records(
            sensors, measurement_name, currenttime)
        if self.args.dryrun:
            self._log_dryrun(voltage_records)
        else:
            logger.info("Writing %d voltage records to backends",
                        len(voltage_records))
            self._safe_write(voltage_records)

        # Fetch bulk reports (informational)
        try:
            reports = self.api.get_reports()
            if len(reports.get("files", [])) > 0:
                logger.info("Bulk reports available: %d files",
                            len(reports["files"]))
        except Exception as e:
            logger.warning("Failed to fetch reports: %s", e)

        # Determine time window (gap-filling in daemon mode)
        if self.args.daemon:
            starttime, stoptime = self._compute_daemon_window(
                mytz, currenttime, sensors)
        else:
            starttime, stoptime = self._compute_oneshot_window(
                mytz, currenttime)

        timelist = build_timelist(starttime, stoptime, self.args.timestep)
        iterations = len(timelist)

        logger.info("Start: %s",
                     datetime.date.strftime(starttime, '%Y-%m-%dT%X%z'))
        logger.info("Stop:  %s",
                     datetime.date.strftime(stoptime, '%Y-%m-%dT%X%z'))
        logger.info("Iterations required: %d", iterations)

        # Fetch and write samples
        for i, window in enumerate(timelist, 1):
            if not self.running:
                logger.info("Shutdown requested, stopping collection")
                break
            self._fetch_and_write_window(
                window, i, iterations, sensors, measurement_name)

    def _compute_daemon_window(self, mytz, currenttime, sensors):
        """Compute time window for daemon mode with per-sensor gap-filling.

        Checks each sensor individually on each backend (using the
        temperature metric as reference) and picks the oldest timestamp
        found.  This ensures that if a single sensor stops reporting,
        the gap for that specific sensor is detected and back-filled.
        """
        poll_backlog_str = self.config['daemon']['poll_backlog']
        poll_backlog_min = parse_backlog(poll_backlog_str)

        # Build sensor ID list (same IDs used as tags in records)
        sensor_ids = list(sensors.keys())

        # Find the oldest last-known timestamp across all sensors × backends
        oldest_ts = None
        oldest_info = None  # for logging: (sensor_id, writer_name)

        for writer in self.writers:
            if not writer.connected or writer._consecutive_failures > 0:
                if not writer.connected:
                    logger.debug("Skipping gap check on %s (not connected)",
                                 type(writer).__name__)
                else:
                    logger.debug("Skipping gap check on %s "
                                 "(%d consecutive write failures)",
                                 type(writer).__name__,
                                 writer._consecutive_failures)
                continue
            for sid in sensor_ids:
                if not self.running:
                    break
                # sensor_id tag is stored as float(key), e.g. "1234567.0"
                sid_tag = str(float(sid))
                try:
                    ts = writer.get_last_timestamp(
                        writer.measurement_name, sensor_id=sid_tag)
                    if ts is not None:
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=datetime.timezone.utc)
                        if oldest_ts is None or ts < oldest_ts:
                            oldest_ts = ts
                            oldest_info = (sid, type(writer).__name__)
                except Exception as e:
                    logger.warning(
                        "Could not query last timestamp for sensor %s "
                        "from %s: %s", sid, type(writer).__name__, e)

        if oldest_ts:
            oldest_ts_local = oldest_ts.astimezone(mytz)
            gap_minutes = (currenttime - oldest_ts_local).total_seconds() / 60

            if gap_minutes > poll_backlog_min:
                sensor_name = sensors.get(
                    oldest_info[0], {}).get('name', oldest_info[0])
                logger.info(
                    "Gap detected: sensor '%s' on %s last seen at %s "
                    "(%.0f min ago), fetching with 1h safety margin",
                    sensor_name, oldest_info[1],
                    oldest_ts_local.strftime('%Y-%m-%dT%X%z'), gap_minutes)
                # Go back 1 hour before the oldest data point as safety margin
                starttime = oldest_ts_local - datetime.timedelta(hours=1)
            else:
                starttime = currenttime - datetime.timedelta(
                    minutes=poll_backlog_min)
        else:
            logger.info("No existing data found, using poll backlog of %s",
                        poll_backlog_str)
            starttime = currenttime - datetime.timedelta(
                minutes=poll_backlog_min)

        return starttime, currenttime

    def _compute_oneshot_window(self, mytz, currenttime):
        """Compute time window for one-shot mode (backward compatible)."""
        if self.args.starttime:
            starttime = datetime.datetime.strptime(
                self.args.starttime, '%Y-%m-%dT%X%z')
        else:
            backlog = parse_backlog(self.args.backlog)
            starttime = currenttime - datetime.timedelta(minutes=backlog)

        if self.args.stoptime:
            stoptime = datetime.datetime.strptime(
                self.args.stoptime, '%Y-%m-%dT%X%z')
        else:
            stoptime = currenttime

        return starttime, stoptime

    def _fetch_and_write_window(self, window, iteration, iterations,
                                sensors, measurement_name):
        """Fetch samples for one time window and write to backend."""
        retrycount = 0
        while True:
            try:
                logger.info("Iteration %d/%d", iteration, iterations)

                # The SensorPush API defaults to ~10 samples per sensor
                # when no limit is specified.  In daemon mode we always
                # want all available data, so use a high limit.
                qlimit = self.args.qlimit
                if qlimit == 0:
                    qlimit = 10000

                samples = self.api.get_samples(
                    window[0], window[1],
                    measures=MEASURES,
                    limit=qlimit,
                    sensors=self.args.sensorlist
                )

                truncated = samples['truncated']
                numsamples = samples['total_samples']
                numsensors = samples['total_sensors']

                logger.info("Request truncated: %s", truncated)
                if truncated:
                    logger.warning(
                        "Response truncated - consider reducing timestep")
                logger.info("Samples: %d, Sensors: %d",
                            numsamples, numsensors)

                records = process_samples(
                    samples, sensors, measurement_name,
                    self.config['my_altitude'], self.args.noconvert)

                if self.args.dryrun:
                    self._log_dryrun(records)
                else:
                    self._safe_write(records)

                # Delay between iterations
                if iterations > 1 and iteration < iterations:
                    logger.info("Sleeping for %d seconds", self.args.delay)
                    if self.args.daemon:
                        self._interruptible_sleep(self.args.delay)
                    else:
                        time.sleep(self.args.delay)
                return

            except Exception as e:
                retrycount += 1
                logger.error("Error in iteration %d/%d: %s",
                             iteration, iterations, e)
                try:
                    logger.debug("Last request status: %s", r.status_code)
                    logger.debug("Last request headers: %s", r.headers)
                    logger.debug("Last request content: %s",
                                 r.content.decode('utf-8'))
                except Exception:
                    pass

                if retrycount >= MAXRETRY:
                    if self.args.daemon:
                        logger.error(
                            "Max retries (%d) reached for iteration %d, "
                            "skipping", MAXRETRY, iteration)
                        return
                    else:
                        logger.error(
                            "Max retries (%d) reached, stopping!",
                            MAXRETRY)
                        raise
                logger.info("Retrying in %ds (attempt %d/%d)",
                            RETRYWAIT, retrycount, MAXRETRY)
                time.sleep(RETRYWAIT)

    def _safe_write(self, records):
        """Write records to all backends with retry and reconnection."""
        all_failed = True
        for writer in self.writers:
            if not self.running:
                return

            # If never connected or previously marked disconnected, try to connect
            if not writer.connected:
                try:
                    logger.info("Attempting to (re)connect %s...",
                                type(writer).__name__)
                    writer.reconnect()
                    logger.info("Reconnected %s successfully",
                                type(writer).__name__)
                except Exception as e:
                    logger.warning("Reconnect %s failed: %s",
                                   type(writer).__name__, e)
                    continue  # skip this backend for this cycle

            for attempt, delay in enumerate([5, 10], 1):
                if not self.running:
                    return
                try:
                    logger.debug("Writing %d records to %s (attempt %d)...",
                                 len(records), type(writer).__name__, attempt)
                    t0 = time.time()
                    writer.write(records)
                    logger.info("Write to %s OK (%d records, %.1fs)",
                                type(writer).__name__, len(records),
                                time.time() - t0)
                    writer._consecutive_failures = 0
                    all_failed = False
                    break
                except Exception as e:
                    logger.error("Write to %s failed (attempt %d/2, %.1fs): %s",
                                 type(writer).__name__, attempt,
                                 time.time() - t0, e)
                    if attempt < 2:
                        time.sleep(delay)
            else:
                writer._consecutive_failures += 1
                logger.error("Failed to write to %s after 2 attempts "
                             "(%d consecutive), skipping batch",
                             type(writer).__name__,
                             writer._consecutive_failures)
                # After repeated failures, force reconnection on next cycle
                if writer._consecutive_failures >= 3:
                    logger.warning("Marking %s for reconnection after %d "
                                   "consecutive failures",
                                   type(writer).__name__,
                                   writer._consecutive_failures)
                    writer._connected = False

        if all_failed and not self.args.daemon:
            raise ConnectionError(
                "Failed to write to any backend after 3 attempts")

    def _log_dryrun(self, records):
        """Log records in dryrun mode."""
        logger.info("--- Data that would have been written ---")
        if self.args.verbose:
            for r in records:
                logger.info("%s", r)
        else:
            for r in records[:5]:
                logger.info("%s", r)
            if len(records) > 5:
                logger.info("... and %d more records", len(records) - 5)
        logger.info("--- End of dryrun data ---")

    def run_once(self):
        """Single collection cycle (backward compatible one-shot mode)."""
        self._connect_writers()
        try:
            self._collect_cycle()
        finally:
            for w in self.writers:
                try:
                    w.close()
                except Exception as e:
                    logger.warning("Error closing %s: %s",
                                   type(w).__name__, e)


# -----------------------------------------------------------------------------
# Display helpers (for --listsensors / --listgateways)
# -----------------------------------------------------------------------------
def display_gateways(gateways):
    for gid in gateways.keys():
        gwname = gateways[gid]["name"]
        print(f'---------------{gwname}---------------')
        print(f'Last alert               : {gateways[gid]["last_alert"]}')
        print(f'Last seen                : {gateways[gid]["last_seen"]}')
        print(f'Message                  : {gateways[gid]["message"]}')
        print(f'Paired                   : {gateways[gid]["paired"]}')
        print(f'Version                  : {gateways[gid]["version"]}')
        print('------------------------------------------------------------')
        print('')


def display_sensors(sensors):
    for sid in sensors.keys():
        sensorname = sensors[sid]["name"]
        print(f'---------------{sensorname}---------------')
        for key in sensors[sid].keys():
            print(f'{key}: {sensors[sid][key]}')
        try:
            float(sensors[sid]["battery_voltage"])
        except (KeyError, TypeError):
            print(f'Failed to get battery_voltage for {sensors[sid]["name"]}')
        try:
            float(sensors[sid]["rssi"])
        except (KeyError, TypeError):
            print(f'Failed to get rssi for {sensors[sid]["name"]}')
        print('------------------------------------------------------------')
        print('')


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    args = parse_args()

    # Setup logging
    setup_logging(level=args.loglevel, log_file=args.logfile)

    # Generate config and exit if requested
    if args.generate_config is not None:
        if args.generate_config:
            path = args.generate_config
        elif args.config:
            path = args.config
        else:
            path = f'{Path.home()}/.sensorpushd.conf'
        if Path(path).is_file():
            print(f'Config file already exists at {path} — not overwriting.')
            return 1
        create_default_config(path)
        print(f'Config template written to {path}')
        return 0

    # Determine config file
    if args.config:
        config_path = args.config
    else:
        homedir = str(Path.home())
        config_path = f'{homedir}/.sensorpushd.conf'
        # Fall back to legacy configs if new one doesn't exist
        if not Path(config_path).is_file():
            for legacy in [f'{homedir}/.sensorpush_vm.conf',
                           f'{homedir}/.sensorpush.conf']:
                if Path(legacy).is_file():
                    config_path = legacy
                    logger.info("Using legacy config: %s", config_path)
                    break

    # Load config
    config = load_config(config_path)

    # Determine backend(s)
    if args.backend:
        backend_names = args.backend  # list from nargs='+'
    else:
        backend_names = [b.strip() for b in config['backend'].split(',')]

    # Handle list-only commands
    if args.listsensors or args.listgateways:
        api = SensorPushAPI(config['login'], config['password'],
                            verify_ssl=VERIFY_SSL,
                            force_ipv4=config['force_ipv4'])
        api.authenticate()
        if args.listgateways:
            display_gateways(api.get_gateways())
        if args.listsensors:
            display_sensors(api.get_sensors())
        return 0

    # Create API client and writers
    api = SensorPushAPI(config['login'], config['password'],
                        verify_ssl=VERIFY_SSL,
                        force_ipv4=config['force_ipv4'])
    logger.info("Active backend(s): %s", ', '.join(backend_names))
    writers = create_writers(backend_names, config)

    # Run
    daemon = SensorPushDaemon(api, writers, config, args)

    if args.daemon:
        daemon.run()
    else:
        daemon.run_once()

    return 0


if __name__ == '__main__':
    sys.exit(main())
