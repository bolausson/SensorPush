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
import re
import sys
import json
import time
import math
import random
import signal
import socket
import logging
import tempfile
import threading
import requests
import datetime
import argparse
import configparser
from abc import ABC, abstractmethod
from pathlib import Path
from requests.adapters import HTTPAdapter


def suppress_insecure_warnings():
    """Silence urllib3's InsecureRequestWarning.

    Only called when a backend is actually configured with
    verify_ssl=False, so an unexpected certificate problem on a
    verified connection still surfaces instead of being hidden.
    """
    try:
        from urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    except Exception:
        pass


def enable_force_ipv4():
    """Force IPv4-only outbound connections for urllib3.

    On networks where IPv6 to api.sensorpush.com blackholes, a plain
    connect() stalls for minutes before falling back.  urllib3 reads
    HAS_IPV6 at call time, so flipping it here affects every later
    connection.  This is process-global, hence a single explicit call
    from main() rather than a constructor side effect.
    """
    try:
        import urllib3.util.connection
        urllib3.util.connection.HAS_IPV6 = False
        return True
    except Exception as e:
        logger.warning("Could not force IPv4: %s", e)
        return False


def sd_notify(state):
    """Send a notification to systemd (if available).

    Uses the NOTIFY_SOCKET environment variable set by systemd for
    Type=notify or WatchdogSec services.  Silently does nothing when
    the socket is not available (e.g. running outside systemd).

    The protocol is newline-delimited ``KEY=value``; callers passing
    free-form text (STATUS=) must sanitize it first - see sd_status().
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


def sd_status(text):
    """Report a one-line human-readable status to systemd.

    Shows up in ``systemctl status`` so a degraded-but-running daemon
    is visible without trawling the journal.
    """
    sd_notify('STATUS=' + str(text).replace('\n', ' ')[:200])


logger = logging.getLogger('sensorpushd')

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
# api.sensorpush.com presents a valid public certificate chain, and the
# account password is sent in the auth request body - always verify it.
# The LAN backends have their own per-section verify_ssl settings.
VERIFY_SSL = True
RETRYWAIT = 60
MAXRETRY = 3
RETRY_DELAYS = [10, 30, 60, 120, 300]
# Stop feeding the systemd watchdog if the main loop makes no progress for
# this many seconds.  Must be larger than any single legitimate blocking
# step (an API call + its retries) but small enough to catch a real hang.
# Derived from WatchdogSec at runtime; this is the floor and the fallback.
WATCHDOG_HANG_THRESHOLD = 900
# Cap on the exponential backoff applied between failed collection cycles.
MAX_CYCLE_BACKOFF = 3600
# Log a failing cycle at CRITICAL (and report it to systemd) once this many
# consecutive failures have accumulated.
CYCLE_ALERT_AFTER = 12
# HTTP status codes worth retrying: transient server-side or rate limiting.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

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

# The SensorPush API hands out each sensor's samples in chunks of roughly 15
# minutes and selects a chunk by its *start* time: a query from T returns
# only chunks that started at or after T, even when an earlier chunk holds
# samples observed after T.  A poll that reaches back only a few minutes
# therefore silently misses most samples - with 10 minutes, measured on a
# real installation, about three quarters of them.  Every window
# that looks back from "now" reaches back at least this far, so each chunk
# is fetched again after it has been completed and uploaded.
#
# For the newest minutes (up to about half an hour) the API also serves live
# copies of the readings at irregular seconds; a later re-read returns only
# the sensor's own samples, on a fixed second of the minute and usually
# within a minute of the copies.  Both are stored on purpose: the live copies
# are the only data for the newest minutes, and some readings are never
# served in any other form.  See the README section "Live and final samples".
MIN_POLL_BACKLOG_MIN = 60

# Defaults for the [DAEMON] section, shared by the unified and legacy loaders.
#
#   poll_backlog  How far every poll reaches back.  Never less than
#       MIN_POLL_BACKLOG_MIN; shorter settings are raised (with a warning).
#   max_consecutive_failures  0 = never give up.  The daemon backs off
#       exponentially instead; a transient upstream outage must not end
#       collection permanently.
#   sensor_return_grace  How long a sensor that has started reporting again
#       must keep reporting before its gap is back-filled, so the gateway has
#       time to upload the sensor's buffered backlog to the cloud first.
#   sensor_stale_after  How far a sensor may fall behind the rest before it
#       is treated as missing rather than merely slow.  Sensors report on
#       their own cadence, so this must be well above normal jitter or every
#       cycle probes every sensor for nothing.
#   gap_lookback  How far back get_last_timestamp() looks when locating
#       existing data.  '0' = no limit.  Must exceed the longest outage the
#       daemon should still be able to recover from.
#   recheck_gaps  When to re-read a gap in a sensor's data, counted from the
#       moment it closed (the sensor reported again).  Gateways upload missed
#       samples late, so one read right after a gap misses them.  '0' (or
#       off, false, no, none, empty) turns the re-checks off; the daemon then
#       behaves exactly as it did without them.  Daemon mode only.
#   recheck_min_gap  The shortest silence that counts as a gap.  At least
#       15m: live copies of one sensor can be almost 13 minutes apart.
#       The defaults of both are in DEFAULT_RECHECK_CFG.
DEFAULT_DAEMON_CFG = {
    'interval': 300,
    'poll_backlog': '1h',
    'max_backfill': '0',
    'deep_scan_interval': '1h',
    'max_consecutive_failures': 0,
    'sensor_return_grace': '1h',
    'gap_lookback': '0',
    'sensor_stale_after': '6h',
}

# Kept out of DEFAULT_DAEMON_CFG, and added to a loaded config only when the
# file sets them, so a config without them loads exactly as before.
DEFAULT_RECHECK_CFG = {
    'recheck_gaps': '1h, 6h, 1d, 3d, 7d',
    'recheck_min_gap': '20m',
}

# -----------------------------------------------------------------------------
# Gap re-checks
# -----------------------------------------------------------------------------
# After an outage the cloud fills in the past late: a gateway uploads a small
# backlog within about an hour, HT1 history can take hours to weeks.  So each
# jump in a sensor's sample stream is recorded as a hole and read again
# RECHECK_GAPS after it closed - see _note_fetched() and _recheck_gaps().
#
# Per cycle, re-reads make at most this many calls and take at most this long,
# including the --delay sleeps between them, so they never hold up the polls.
RECHECK_CALLS_PER_CYCLE = 6
RECHECK_SECONDS_PER_CYCLE = 120
# Cap of the back-off after a failed re-read call.
RECHECK_BACKOFF_MAX = 3600
# A sensor that keeps dropping out cannot pile up holes without bound: beyond
# this many, its two closest holes are merged (nothing is dropped).
RECHECK_HOLES_PER_SENSOR = 40
# Nor can many sensors together: beyond this many holes in all, the closest
# holes of the sensor with the most are merged.  state.json is rewritten
# every cycle, and 100 sensors at the per-sensor cap would make it ~0.7 MiB.
RECHECK_HOLES_MAX = 1000
# Unread holes of one sensor this close together are read as one.
RECHECK_MERGE_WITHIN = 6 * 3600
# A hole is read from its start - 1h to its end + 1h.  The sensor's own
# samples just before and after a gap often reach the cloud only with the
# backlog; until then the database holds only live copies there.
RECHECK_MARGIN = 3600
# A re-read call reads a cell plus this much on either side, as every other
# window overlaps the next: the newest hour is selected by chunk start.
RECHECK_CALL_PAD = 30 * 60
# A hole that is almost due rides along in a call that a due hole triggered:
# up to this share of its offset early, and for the last offset at most
# RECHECK_EARLY_LAST_MAX early, so the horizon stays where it is.
RECHECK_EARLY_SHARE = 0.25
RECHECK_EARLY_LAST_MAX = 30 * 60
# Polls at most this far apart see the data live: the API serves a live copy
# of each reading for about 30 minutes, so in such a stretch every reading
# was seen, and a jump in it is a real gap.
RECHECK_LIVE_TOLERANCE = 25 * 60
# A fetched window whose stop is at most this far in the past reaches the
# present, i.e. it was served live copies.
RECHECK_PRESENT_WITHIN = 10 * 60
# Threshold for sensors that skip unchanged readings (HT1) in data that was
# not seen live: their own records have gaps of up to 63 min (the most in 103
# days of 12 sensors), so a shorter jump there proves nothing.
RECHECK_SETTLED_MIN_GAP = 90 * 60
# Types that record every minute without skipping: no own record is more
# than 65 s from the next (148k samples), so any jump over RECHECK_MIN_GAP
# is a gap, live or not.
ONE_PER_MINUTE_TYPES = frozenset({'HTP.xw', 'HT.w'})
# Days of samples a sensor keeps on board.  Older parts of a long gap can
# never reach the cloud and are not re-read.  Unknown types: the longest.
STORAGE_DAYS = {'HT1': 20, 'HTP.xw': 30, 'HT.w': 45}
STORAGE_DAYS_UNKNOWN = 45
# A hole clamped to on-board storage keeps at most this much more than the
# storage, so that its cells line up with those of the other sensors of an
# outage (see _storage_start); a hole restored from state.json may be this
# much longer than the storage.  Not a cell: cells shrink after a hole was
# saved (a lowered -t, a truncated answer).
RECHECK_SPAN_SLACK = 86400
# Only samples at least this old count when deciding whether a re-read found
# more than any before: live copies vanish after ~30 minutes, so counting
# them could hide own samples that arrived meanwhile.
RECHECK_SETTLED_AGE = 35 * 60
# Re-read records are written in slices of at most this many: one write of
# a long hole could exceed InfluxDB 3's 10 MiB request limit or a
# VictoriaMetrics timeout.
RECHECK_WRITE_SLICE = 5000
# A hole whose re-read fails this many times in a row is dropped.
RECHECK_HOLE_MAX_ERRORS = 3
# A one-cell hole of a ONE_PER_MINUTE_TYPES sensor is complete - no more
# re-reads - when its interior has no step over RECHECK_COMPLETE_STEP and
# reaches within RECHECK_COMPLETE_EDGE of both ends (own records are at most
# 65 s apart).
RECHECK_COMPLETE_STEP = 90
RECHECK_COMPLETE_EDGE = 180
# A hole longer than this whose first re-read found nothing is next read at
# the first offset of at least RECHECK_EMPTY_SKIP_TO: a sensor whose trip
# data never reaches the cloud then costs 3 passes, not 5, while late HT1
# history is still caught by the 3d and 7d reads.
RECHECK_EMPTY_LONG = 86400
RECHECK_EMPTY_SKIP_TO = 3 * 86400
# Values of RECHECK_GAPS that turn the re-checks off (any case).
RECHECK_OFF_VALUES = ('', '0', 'off', 'false', 'no', 'none')
# RECHECK_GAPS offsets are at most 90 days (no sensor keeps more than 45 on
# board) and at least 30 minutes: sooner, the gateway has hardly begun to
# upload, and the polls re-read the newest hour anyway.
RECHECK_OFFSET_MAX_MIN = 90 * 24 * 60
RECHECK_OFFSET_FLOOR_MIN = 30
# RECHECK_MIN_GAP floor: live copies of one sensor can be 12.8 min apart.
RECHECK_MIN_GAP_FLOOR_MIN = 15
# The API hands out a sensor's samples in chunks of about RECHECK_CHUNK and
# selects the newest RECHECK_CHUNK_SELECT of data by a chunk's start: a
# window that starts there can miss up to one chunk after its start.
RECHECK_CHUNK = 15 * 60
RECHECK_CHUNK_SELECT = 3600
# Shortest cell, in minutes.  A truncated re-read answer halves the cells of
# the holes it served, down to this.
RECHECK_CELL_MIN = 30
# A cell whose re-read calls fail this many times in a row is skipped for
# the current pass (the next pass reads it again): a window that can never
# be read or written must not hold up the other holes.  From its second
# failed call on, a cell is read alone, on the back-off that all cells read
# alone share (see _recheck_gaps).
RECHECK_CELL_MAX_FAILS = 3
# The last pass has no next one: there a cell is not skipped but kept, and
# tried again alone (kept cells take every other read alone after the
# others, unless a backend refused their records, see _recheck_pick); only
# a call that fails more than this long after the last offset gives it up,
# so the tries of that day can outlast a spell of failed calls.  A pass is
# also the last when it began after the last offset: the offsets after it
# collapse into it.
RECHECK_LAST_PASS_GRACE = 86400
# Times restored from state.json lie between 2015-01-01 (no SensorPush data
# is older) and one day ahead of the clock; anything else is damage.
RECHECK_EPOCH_MIN = 1420070400
RECHECK_FUTURE_SLACK = 86400


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
            'sensor_uid': config['INFLUXDBCONF'].getboolean('SENSOR_UID', fallback=False),
        },
        'influxdb3': None,
        'victoriametrics': None,
        'daemon': _read_recheck_keys(config, dict(DEFAULT_DAEMON_CFG)),
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
            'sensor_uid': config['VICTORIAMETRICSCONF'].getboolean('SENSOR_UID', fallback=False),
        },
        'daemon': _read_recheck_keys(config, dict(DEFAULT_DAEMON_CFG)),
    }


def _read_recheck_keys(config, daemon_cfg):
    """Add RECHECK_GAPS / RECHECK_MIN_GAP from [DAEMON], if the file sets them.

    The only [DAEMON] keys a legacy config file honours, so its users can
    turn the re-checks off too.  Keys the file does not set stay out of the
    dict; the daemon then uses DEFAULT_RECHECK_CFG.
    """
    for key in DEFAULT_RECHECK_CFG:
        if config.has_option('DAEMON', key.upper()):
            # Raw: a '%' in the value would fail the interpolation, and with
            # it loading the file in every mode.  The parsers reject it in
            # daemon mode with a one-line error; one-shot mode ignores it.
            daemon_cfg[key] = config.get('DAEMON', key.upper(), raw=True)
    return daemon_cfg


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
            'poll_backlog': config.get('DAEMON', 'POLL_BACKLOG', fallback='1h'),
            'max_backfill': config.get('DAEMON', 'MAX_BACKFILL', fallback='0'),
            'deep_scan_interval': config.get('DAEMON', 'DEEP_SCAN_INTERVAL', fallback='1h'),
            'max_consecutive_failures': config.getint(
                'DAEMON', 'MAX_CONSECUTIVE_FAILURES', fallback=0),
            'sensor_return_grace': config.get(
                'DAEMON', 'SENSOR_RETURN_GRACE', fallback='1h'),
            'gap_lookback': config.get('DAEMON', 'GAP_LOOKBACK', fallback='0'),
            'sensor_stale_after': config.get(
                'DAEMON', 'SENSOR_STALE_AFTER', fallback='6h'),
        },
    }
    _read_recheck_keys(config, cfg['daemon'])

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
            'sensor_uid': config.getboolean('INFLUXDB2', 'SENSOR_UID', fallback=False),
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
            'sensor_uid': config.getboolean('INFLUXDB3', 'SENSOR_UID', fallback=False),
        }

    if 'VICTORIAMETRICS' in config:
        cfg['victoriametrics'] = {
            'measurement_name': config.get('VICTORIAMETRICS', 'MEASUREMENT_NAME', fallback='SensorPush'),
            'url': config.get('VICTORIAMETRICS', 'URL', fallback='http://localhost:8428'),
            'verify_ssl': config.getboolean('VICTORIAMETRICS', 'VERIFY_SSL', fallback=False),
            'sensor_uid': config.getboolean('VICTORIAMETRICS', 'SENSOR_UID', fallback=False),
        }

    return cfg


# Written as text rather than through ConfigParser, which would drop the
# comments and lower-case the keys.  Full-line comments only: values must
# not carry inline comments, ConfigParser would read them as part of the
# value.
DEFAULT_CONFIG_TEMPLATE = """\
[SENSORPUSHAPI]
LOGIN = SensorPush login (email)
PASSWORD = SensorPush password

[BACKEND]
# One or more of: influxdb2, influxdb3, victoriametrics (comma-separated).
# All backends are written with the MEASUREMENT_NAME of the first one.
TYPE = victoriametrics

# Each backend section accepts SENSOR_UID.  When True, every point written to
# that backend also carries the full SensorPush sensor id as the tag
# sensor_uid.  sensor_id itself never changes (it stays str(float(id)), see
# the README), so existing queries keep working, but enabling it starts one
# new series per sensor, just like renaming a sensor does.  Read the README
# section "Storing the full sensor id" before enabling it on a database
# that already holds data.

[INFLUXDB2]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8086
TOKEN = your_influxdb2_token
ORG = your_org
BUCKET = sensorpush
VERIFY_SSL = False
SENSOR_UID = False

[INFLUXDB3]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8181
DATABASE = sensorpush
TOKEN = your_influxdb3_token
VERIFY_SSL = False
SENSOR_UID = False

[VICTORIAMETRICS]
MEASUREMENT_NAME = SensorPush
URL = http://localhost:8428
VERIFY_SSL = False
SENSOR_UID = False

[DAEMON]
INTERVAL = 300
# How far every poll reaches back.  The SensorPush API selects samples in
# ~15-minute chunks by their start time, so shorter windows silently miss
# data; anything below 1h is raised to 1h.
POLL_BACKLOG = 1h
# 0 = back-fill without limit.  Raising this off 0 caps how far the
# daemon can recover after a long outage - see GAP_LOOKBACK too.
MAX_BACKFILL = 0
DEEP_SCAN_INTERVAL = 1h
# 0 = never stop collecting; back off exponentially instead.
MAX_CONSECUTIVE_FAILURES = 0
# Settle time before back-filling a sensor that started reporting again.
SENSOR_RETURN_GRACE = 1h
# 0 = search the full history when locating existing data.
GAP_LOOKBACK = 0
# How far a sensor may lag the others before it counts as missing.
SENSOR_STALE_AFTER = 6h
# When to re-read a gap after it closed (gateways upload late); 0 = off.
# RECHECK_GAPS = 1h, 6h, 1d, 3d, 7d
# The shortest silence of a sensor that counts as a gap.
# RECHECK_MIN_GAP = 20m

[MISC]
MY_ALTITUDE = 0.0
# Set to True on networks where IPv6 to api.sensorpush.com blackholes
# (symptom: connections hang for minutes instead of failing fast).
FORCE_IPv4 = False
"""


def create_default_config(path):
    """Write a default config template."""
    with open(path, 'w') as f:
        f.write(DEFAULT_CONFIG_TEMPLATE)


# -----------------------------------------------------------------------------
# CLI argument parsing
# -----------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description='Unified SensorPush daemon - queries the SensorPush API and '
                    'stores readings in InfluxDB 2, InfluxDB 3, and/or VictoriaMetrics')
    parser.add_argument(
        '-s', '--start', dest='starttime', default='', type=str,
        help='Start query at time (e.g. "2019-07-25T00:10:41+0200"). Start '
             '~15 min before the first sample you need: the API selects '
             'samples in chunks by their start time')
    parser.add_argument(
        '-p', '--stop', dest='stoptime', default='', type=str,
        help='Stop query at time (e.g. "2019-07-26T00:10:41+0200")')
    parser.add_argument(
        '-b', '--backlog', dest='backlog', default='1d', type=str,
        help='Historical data to fetch (default 1d, at least 1h) - format: '
             '<number>[m|h|d|w|M|Y]')
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


_OFFSET_NO_COLON = re.compile(r'([+-])(\d{2})(\d{2})$')
_OBSERVED_FORMATS = ('%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z',
                     '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S')


def _observed_epoch(value):
    """Epoch seconds of an API 'observed' time or a daemon window bound.

    Takes '2026-11-08T10:00:00.000Z', '+00:00' and '+0100' offsets (the
    window strings are '%Y-%m-%dT%X%z') and naive strings, read as UTC.
    Returns None for anything unparseable.  Before Python 3.11,
    fromisoformat() accepts neither a trailing 'Z' nor '+HHMM', so both are
    rewritten first; strptime() covers what is left (e.g. fractions that are
    not 3 or 6 digits long).
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text[-1:] in ('Z', 'z'):
        text = text[:-1] + '+00:00'
    text = _OFFSET_NO_COLON.sub(r'\1\2:\3', text)
    try:
        dt = datetime.datetime.fromisoformat(text)
    except ValueError:
        dt = None
        for fmt in _OBSERVED_FORMATS:
            try:
                dt = datetime.datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    try:
        return dt.timestamp()
    except (OverflowError, OSError, ValueError):
        return None


def _duration_min(text):
    """parse_backlog(), but None instead of an exception."""
    try:
        return parse_backlog(text)
    except (ValueError, KeyError, IndexError, TypeError):
        return None


def parse_recheck_gaps(value):
    """RECHECK_GAPS -> its offsets in seconds, sorted and distinct; [] = off.

    Raises ValueError with a one-line reason if the value is invalid.
    """
    text = str(value).strip()
    if text.lower() in RECHECK_OFF_VALUES:
        return []
    minutes = []
    for item in text.split(','):
        item = item.strip()
        m = _duration_min(item)
        if m is None:
            raise ValueError(
                f"RECHECK_GAPS = {value!r}: {item!r} is not a time such as "
                f"30m, 6h or 3d (0 turns gap re-checks off)")
        if m <= 0 or m > RECHECK_OFFSET_MAX_MIN:
            raise ValueError(
                f"RECHECK_GAPS = {value!r}: {item!r} is not more than 0 and "
                f"at most 90d (0 turns gap re-checks off)")
        minutes.append(m)
    if min(minutes) < RECHECK_OFFSET_FLOOR_MIN:
        logger.warning(
            "RECHECK_GAPS = %s: offsets under %dm are raised to %dm; sooner, "
            "the gateway has hardly begun to upload", value,
            RECHECK_OFFSET_FLOOR_MIN, RECHECK_OFFSET_FLOOR_MIN)
        minutes = [max(m, RECHECK_OFFSET_FLOOR_MIN) for m in minutes]
    return sorted({m * 60 for m in minutes})


def parse_recheck_min_gap(value):
    """RECHECK_MIN_GAP -> seconds.  Raises ValueError if it is invalid."""
    m = _duration_min(str(value).strip())
    if m is None or m <= 0:
        raise ValueError(
            f"RECHECK_MIN_GAP = {value!r} is not a time such as 20m or 1h")
    if m < RECHECK_MIN_GAP_FLOOR_MIN:
        logger.warning(
            "RECHECK_MIN_GAP = %s is raised to %dm: live copies of one sensor "
            "can be almost 13 minutes apart, so a shorter silence is not a gap",
            value, RECHECK_MIN_GAP_FLOOR_MIN)
        m = RECHECK_MIN_GAP_FLOOR_MIN
    return m * 60


# -----------------------------------------------------------------------------
# Stored sensor ids
# -----------------------------------------------------------------------------
# The sensor_id tag has always been written as str(float(<API id>)): Python's
# shortest repr of the nearest double.  SensorPush ids carry about 20
# significant digits and a double keeps 15-17, so the stored value is a
# shortened form of the id, e.g.
#
#     '123456.7890123456789012' -> '123456.78901234567'
#
# The part before the dot (the deviceId) is always kept in full.  The
# mapping is deterministic, so any API id can be turned into its stored
# value; `sensorpushd.py -l` prints both.
#
# Do not change this format.  Every existing series, every user query and
# alert filtering on sensor_id, and the daemon's own gap detection (which
# looks up the newest stored point by this value) depend on it.  A different
# value starts new series and makes the gap check blind to all existing
# data.  The full id can be stored *in addition*, as the opt-in sensor_uid
# tag (SENSOR_UID in a backend's config section).
_non_numeric_ids_reported = set()


def sensor_id_tag(api_id):
    """Return the sensor_id tag value stored for a SensorPush API id.

    A non-numeric id (none is known to exist) is stored unchanged rather
    than failing the whole batch, and reported once.
    """
    try:
        value = float(api_id)
    except (TypeError, ValueError):
        value = None
    if value is None or not math.isfinite(value):
        if api_id not in _non_numeric_ids_reported:
            _non_numeric_ids_reported.add(api_id)
            logger.error("Sensor id %r is not numeric; storing it unchanged "
                         "as sensor_id", api_id)
        return str(api_id)
    return str(value)


def find_sensor_id_collisions(sensor_ids):
    """Return {stored sensor_id: [API ids]} for values shared by several ids.

    Two ids collide only if they round to the same double, which needs the
    same deviceId and the same first 8-12 digits after the dot - not a
    practical risk, but cheap to check.
    """
    by_tag = {}
    for sid in sensor_ids:
        by_tag.setdefault(sensor_id_tag(sid), []).append(str(sid))
    return {tag: sorted(ids) for tag, ids in by_tag.items() if len(ids) > 1}


# -----------------------------------------------------------------------------
# Backend Writers
# -----------------------------------------------------------------------------
class BaseWriter(ABC):
    """Abstract base class for database backend writers."""

    def __init__(self):
        self._connected = False
        self._consecutive_failures = 0
        # Opt-in (SENSOR_UID): also store the full API id as tag sensor_uid.
        self.sensor_uid = False

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
    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        """Query the backend for the most recent data point timestamp.

        If sensor_id is given, returns the last timestamp for that
        specific sensor (using the temperature metric as reference).
        ``lookback_min`` bounds how far back to search; 0 searches the
        full history.  A bound shorter than the actual gap makes the
        sensor look like it has no data at all, which silently disables
        back-filling - so the caller derives it from max_backfill.

        Returns a datetime, or None if the backend holds no matching data.
        Raises if the backend could not be asked: a failed lookup must
        never look like "no data", because the caller would then fetch
        only the poll backlog and silently drop an outage.
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
        self.sensor_uid = bool(config.get('sensor_uid', False))
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
            verify_ssl=self.verify_ssl,
            timeout=15_000  # ms; the library default (10s) is undeclared
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()
        self._connected = True
        logger.info("Connected to InfluxDB 2 at %s", self.url)

    def write(self, records):
        if not records:
            return
        self.write_api.write(bucket=self.bucket, org=self.org, record=records)

    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        # Errors propagate on purpose - see BaseWriter.get_last_timestamp.
        sensor_filter = ''
        if sensor_id is not None:
            sensor_filter = (
                f' |> filter(fn: (r) => r.sensor_id == "{sensor_id}")'
            )
        # 0 = unbounded: start from the Unix epoch rather than a
        # relative window, so a sensor stale for longer than the
        # window is still found.
        start = f'-{int(lookback_min)}m' if lookback_min else '1970-01-01T00:00:00Z'
        # last() runs per series, which is index-cheap.  A sensor that
        # was ever renamed (or has SENSOR_UID switched on) has several
        # series under the same sensor_id, so this returns several rows -
        # take the newest.  (Collapsing with group()+sort() in Flux instead
        # would sort the entire history: ~26s across 18 sensors versus ~1s.)
        query = (
            f'from(bucket: "{self.bucket}")'
            f' |> range(start: {start})'
            f' |> filter(fn: (r) => r._measurement == "{measurement_name}"'
            f'    and r._field == "temperature")'
            f'{sensor_filter}'
            f' |> last()'
        )
        tables = self.query_api.query(query, org=self.org)
        newest = None
        for table in tables:
            for record in table.records:
                ts = record.get_time()
                if ts is not None and (newest is None or ts > newest):
                    newest = ts
        return newest

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
        self.sensor_uid = bool(config.get('sensor_uid', False))
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
            write_client_options=wco,
            verify_ssl=self.verify_ssl
        )
        self._connected = True
        logger.info("Connected to InfluxDB 3 at %s", self.url)

    def write(self, records):
        if not records:
            return
        self.client.write(record=records)

    # Windows (minutes) searched before an unbounded query - see
    # get_last_timestamp().  The first two stay below InfluxDB 3 Core's
    # default query file limit (432 files, about three days of 10-minute
    # files); wider ones may exceed it on a long history.
    SEARCH_WINDOWS_MIN = (24 * 60, 2 * 24 * 60, 30 * 24 * 60)

    # "table 'public.iox.SensorPush' not found", "database not found: x",
    # "Database 'x' not found" - but not e.g. "parquet file not found".
    _MISSING_RE = re.compile(
        r"\b(?:table|database|namespace)\s*"
        r"(?:'[^']*'|\"[^\"]*\"|`[^`]*`|[\w.\-]+)?\s*not found\b",
        re.IGNORECASE)

    @classmethod
    def _is_missing(cls, exc):
        """True if InfluxDB 3 says the table or database does not exist yet.

        That is a genuine "no data" (a fresh install before its first
        write), not a failed lookup.
        """
        return bool(cls._MISSING_RE.search(str(exc)))

    def _max_time(self, measurement_name, sensor_id, minutes):
        conds = []
        if minutes:
            conds.append(f"time > now() - INTERVAL '{int(minutes)} minutes'")
        if sensor_id is not None:
            conds.append(f"sensor_id = '{sensor_id}'")
        where = f" WHERE {' AND '.join(conds)}" if conds else ''
        query = (
            f'SELECT max(time) AS last_time FROM "{measurement_name}"'
            f'{where}'
        )
        try:
            result = self.client.query(query)
        except Exception as e:
            if self._is_missing(e):
                return None
            raise
        if result is not None and len(result) > 0:
            last_time = result.column('last_time')[0].as_py()
            if last_time:
                return last_time
        return None

    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        # Errors propagate on purpose - see BaseWriter.get_last_timestamp.
        if lookback_min:
            return self._max_time(measurement_name, sensor_id, lookback_min)
        # Unbounded.  max(time) without a time predicate has to open every
        # Parquet file of the table, and InfluxDB 3 Core refuses queries
        # that would scan more files than its query file limit - which a
        # long history exceeds.  An active sensor is found in the first,
        # cheap window; only stale sensors need the wider ones.
        for minutes in self.SEARCH_WINDOWS_MIN:
            ts = self._max_time(measurement_name, sensor_id, minutes)
            if ts is not None:
                return ts
        return self._max_time(measurement_name, sensor_id, 0)

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
        self.sensor_uid = bool(config.get('sensor_uid', False))
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

    def get_last_timestamp(self, measurement_name, sensor_id=None,
                           lookback_min=0):
        # Errors propagate on purpose - see BaseWriter.get_last_timestamp.
        #
        # tlast_over_time() (MetricsQL, VictoriaMetrics >= v1.52) returns
        # the time of the newest raw sample as its *value*, value[1].  The
        # timestamp in value[0] is only the query's evaluation time - "now"
        # - so reading it (as an earlier last_over_time() version did) made
        # every stored series look current: an outage was never back-filled
        # and a silent sensor was never noticed.
        sensor_filter = ''
        if sensor_id is not None:
            sensor_filter = f'sensor_id="{sensor_id}"'
        url = f'{self.url}/api/v1/query'
        metric = f'{measurement_name}_temperature'
        # MetricsQL has no unbounded lookbehind; 10y stands in for
        # "no limit" and comfortably exceeds any real retention.
        window = f'{int(lookback_min)}m' if lookback_min else '10y'
        query_str = f'tlast_over_time({metric}{{{sensor_filter}}}[{window}])'
        response = self.session.get(
            url,
            params={'query': query_str},
            verify=self.verify_ssl,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        if data.get('status') != 'success':
            raise RuntimeError(
                f"VictoriaMetrics query failed: {data.get('error') or data}")
        results = data.get('data', {}).get('result', [])
        if not results:
            return None
        # One result per label set, so a renamed sensor (or one with
        # SENSOR_UID switched on) yields several series under the same
        # sensor_id - take the newest, not the first.
        ts = max(float(r['value'][1]) for r in results)
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

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
class AuthCredentialsError(Exception):
    """The API rejected the credentials themselves.

    Distinct from a transient outage: retrying will not help, so callers
    should surface this rather than burning the retry budget on it.
    """


class SensorPushAPI:
    """Client for the SensorPush cloud API."""

    def __init__(self, login, password, verify_ssl=True, sleep_func=None):
        self.login = login
        self.password = password
        self.verify_ssl = verify_ssl
        # Injected by the daemon so retry sleeps stay interruptible and keep
        # feeding the watchdog.  Defaults to a plain sleep for one-shot use.
        self.sleep = sleep_func or time.sleep
        self.session = requests.Session()
        self.session.mount(API_URL_BASE, HTTPAdapter(max_retries=0))
        self.access_token = None
        self._token_time = None
        self._auth_header = None

    def _auth_post(self, url, data, what):
        """POST one step of the OAuth flow, retrying transient failures.

        Both auth steps go through here.  A 4xx other than 429 means the
        request itself is wrong (bad credentials, malformed body) and is
        raised immediately - only genuinely transient conditions consume
        retries, with exponential backoff plus jitter so a fleet of
        clients does not resynchronise on a recovering server.
        """
        last = None
        for attempt in range(1, MAXRETRY + 1):
            logger.info("%s - try %d/%d", what, attempt, MAXRETRY)
            try:
                r = self.session.post(url, headers=HTTP_OA_HEAD, data=data,
                                      verify=self.verify_ssl, timeout=30)
                if r.status_code == 200:
                    return r
                last = f"status {r.status_code}"
                if r.status_code in (401, 403):
                    raise AuthCredentialsError(
                        f"{what} rejected the credentials (status "
                        f"{r.status_code}) - check login/password")
                if r.status_code not in RETRYABLE_STATUS:
                    raise ConnectionError(f"{what} failed with status "
                                          f"{r.status_code}")
                logger.error("%s failed with status %d (retryable)",
                             what, r.status_code)
            except requests.exceptions.RequestException as e:
                last = str(e)
                logger.warning("Connection error during %s: %s", what, e)

            if attempt >= MAXRETRY:
                break
            # 20s, 40s, 80s ... capped, plus up to 25% jitter.
            delay = min(20 * 2 ** (attempt - 1), 160)
            delay = int(delay * (1 + random.random() * 0.25))
            logger.info("Retrying %s in %ds", what, delay)
            self.sleep(delay)

        raise ConnectionError(
            f"{what} failed after {MAXRETRY} attempts (last: {last})")

    def authenticate(self):
        """Perform the full OAuth authentication flow."""
        http_data = json.dumps({'email': self.login, 'password': self.password})

        r = self._auth_post(API_URL_OA_AUTH, http_data,
                            "Fetching API oauth authorization string")
        auth = r.content.decode('utf-8')

        r = self._auth_post(API_URL_OA_ATOK, auth,
                            "Fetching API oauth access token")
        self.access_token = json.loads(r.content.decode('utf-8'))['accesstoken']
        self._token_time = time.time()
        self._auth_header = {
            'accept': 'application/json',
            'Authorization': self.access_token
        }
        logger.info("Authentication successful")

    def _ensure_auth(self):
        """Re-authenticate if token is likely expired (>55 min)."""
        if (self._token_time is None or
                time.time() - self._token_time > 3300):
            self.authenticate()

    def _post(self, url, data=None):
        """Make an authenticated POST request.

        A 401/403 here means the cached token expired earlier than the
        55-minute assumption, so re-authenticate once and retry rather
        than failing the whole cycle.
        """
        self._ensure_auth()
        http_data = json.dumps(data or {})
        for attempt in (1, 2):
            r = self.session.post(url, headers=self._auth_header,
                                  data=http_data, verify=self.verify_ssl,
                                  timeout=60)
            if r.status_code == 200:
                return json.loads(r.content.decode('utf-8'))
            if r.status_code in (401, 403) and attempt == 1:
                logger.info("Token rejected by %s, re-authenticating", url)
                self._token_time = None
                self._ensure_auth()
                continue
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
                'sensor_id': sensor_id_tag(sensors[sid]["id"]),
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
                    'sensor_id': sensor_id_tag(key),
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
# Gap re-checks: holes and their cells
# -----------------------------------------------------------------------------
# A hole is a dict, kept in state.json under rechecks.holes with these keys:
#   sid        API sensor id
#   start/end  observed times (epoch) of the last sample before and the
#              first sample after the gap; the data of interest lies
#              strictly between them, in the interior
#   closed_at  when the hole was registered (time.time())
#   done       completed re-read passes
#   pos        next cell of the current pass; 0 between passes
#   cell       cell length in minutes, set at registration; halved when a
#              re-read answer was truncated
#   counts     per cell, the most settled interior samples any read found
#   probe      None for a gap.  A number (seconds) for a stretch a fetched
#              window did not reach although no gap was seen: its first
#              pass writes what it holds, then it is dropped if no step in
#              it is longer than that, and read on as a gap otherwise.  Two
#              probes merged by a cap stay a probe (the smaller number).
# Keys starting with '_' exist in memory only.
#
# The read range [start - RECHECK_MARGIN, end + RECHECK_MARGIN] is split into
# cells of `cell` minutes, one call each, read in order: a long hole is read
# over several cycles, and a count is always compared over the same range.
HOLE_KEYS = ('sid', 'start', 'end', 'closed_at', 'done', 'pos', 'cell',
             'counts', 'probe')
# In-memory keys of a hole, dropped whenever its cells or its pass change.
# '_fail' is the failure record of the cell about to be read (see
# _fail_record); '_since' is when the pass began: its first cell read or
# skipped (see _last_pass).
_HOLE_PASS_KEYS = ('_found', '_new', '_chain', '_fail', '_partial',
                   '_qwarned', '_since')
_HOLE_SPAN_MAX = (max(list(STORAGE_DAYS.values()) + [STORAGE_DAYS_UNKNOWN])
                  + 1) * 86400


def _hole_ncells(hole):
    """Number of cells of a hole's read range."""
    span = hole['end'] - hole['start'] + 2 * RECHECK_MARGIN
    # The tolerance keeps float noise from adding an empty last cell.
    return max(1, int(math.ceil(span / (hole['cell'] * 60.0) - 1e-9)))


def _hole_cell(hole, k):
    """Bounds (epoch) of cell k of a hole; the last one ends at end + margin."""
    c = hole['start'] - RECHECK_MARGIN + k * hole['cell'] * 60
    if k >= _hole_ncells(hole) - 1:
        return c, hole['end'] + RECHECK_MARGIN
    return c, c + hole['cell'] * 60


def _is_number(v):
    """A finite JSON number (bool is an int in Python, but not one here)."""
    if not isinstance(v, (int, float)) or isinstance(v, bool):
        return False
    try:
        return math.isfinite(v)
    except OverflowError:               # an int too large for a float
        return False


def _is_count(v):
    """A non-negative JSON integer."""
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


def _plausible_time(v, now):
    """An epoch from state.json that this daemon could have written."""
    return _is_number(v) and RECHECK_EPOCH_MIN <= v <= now + RECHECK_FUTURE_SLACK


def _valid_hole(entry, now):
    """A hole read back from state.json, checked field by field.

    Returns a clean copy, or None if the entry is malformed - also when it
    holds values the daemon never writes (times before 2015 or more than a
    day ahead of ``now``), which would break the date arithmetic or keep a
    hole that is never due.  Counts that do not fit the hole's cells are
    reset (and the pass restarts) rather than trusted.
    """
    try:
        return _checked_hole(entry, now)
    except (TypeError, ValueError, OverflowError):
        return None


def _checked_hole(entry, now):
    if not isinstance(entry, dict):
        return None
    sid = entry.get('sid')
    if not isinstance(sid, str) or not sid:
        return None
    if not all(_plausible_time(entry.get(k), now)
               for k in ('start', 'end', 'closed_at')):
        return None
    if not (_is_count(entry.get('done')) and _is_count(entry.get('pos'))
            and _is_count(entry.get('cell'))
            and entry['cell'] >= RECHECK_CELL_MIN):
        return None
    probe = entry.get('probe')
    if probe is not None and not (_is_number(probe) and probe > 0):
        return None
    hole = {'sid': sid, 'start': float(entry['start']),
            'end': float(entry['end']), 'closed_at': float(entry['closed_at']),
            'done': entry['done'], 'pos': entry['pos'], 'cell': entry['cell'],
            'probe': None if probe is None else float(probe)}
    # The detector never registers a hole longer than on-board storage plus
    # RECHECK_SPAN_SLACK (see _storage_start); one that is (a damaged file)
    # would cost a call per cell for nothing.  The bound must not depend on
    # the hole's cell: that shrinks after the hole was saved.
    if not (0 < hole['end'] - hole['start']
            <= _HOLE_SPAN_MAX + RECHECK_SPAN_SLACK):
        return None
    n = _hole_ncells(hole)
    counts = entry.get('counts')
    if (isinstance(counts, list) and len(counts) == n
            and all(_is_count(c) for c in counts)):
        hole['counts'] = list(counts)
    else:
        hole['counts'] = [0] * n
        hole['pos'] = 0
    if hole['pos'] >= n:
        hole['pos'] = 0
    return hole


def _chain_steps(hole, chain):
    """The steps of a pass's settled interior samples, from start to end.

    ``chain`` is what _advance_hole() collected over a pass that read every
    cell in order.  Returns (start to first sample, largest step between
    samples, last sample to end), or None when there was no interior sample.
    """
    if chain['last'] is None:
        return None
    return (chain['first'], chain['inner'], hole['end'] - chain['last'])


def _interior_complete(hole, chain):
    """True if a pass found the interior of a hole complete.

    For sensors that record every minute: no step between samples over
    RECHECK_COMPLETE_STEP, and none at either end over RECHECK_COMPLETE_EDGE.
    """
    steps = _chain_steps(hole, chain)
    return (steps is not None and steps[0] <= RECHECK_COMPLETE_EDGE
            and steps[1] <= RECHECK_COMPLETE_STEP
            and steps[2] <= RECHECK_COMPLETE_EDGE)


def _largest_step(hole, chain):
    """The longest stretch without a sample that a pass found in a hole."""
    steps = _chain_steps(hole, chain)
    return hole['end'] - hole['start'] if steps is None else max(steps)


def _fmt_offset(seconds):
    """3600 -> '1h', 86400 -> '1d', 1800 -> '30m' (for logs)."""
    minutes = int(round(seconds / 60.0))
    if minutes and minutes % 1440 == 0:
        return f'{minutes // 1440}d'
    if minutes and minutes % 60 == 0:
        return f'{minutes // 60}h'
    return f'{minutes}m'


def _fmt_duration(seconds):
    """The length of a gap, for logs: '130 min', '7.5 h', '3.2 days'."""
    if seconds < 3 * 3600:
        return f'{seconds / 60:.0f} min'
    if seconds < 2 * 86400:
        return f'{seconds / 3600:.1f} h'
    return f'{seconds / 86400:.1f} days'


def _fmt_failed(calls):
    """'failed once' / 'failed 3 times in a row', for logs: the failed
    calls made for a cell in this pass.  Not its failure count: a carry
    starts that one short of RECHECK_CELL_MAX_FAILS, a regroup sets it
    back."""
    if calls == 1:
        return 'failed once'
    return f'failed {calls} times in a row'


def _fmt_time(t):
    """An epoch in local time, for logs (never raises)."""
    try:
        return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M')
    except (OverflowError, OSError, ValueError, TypeError):
        return repr(t)


def _fmt_span(start, end, now):
    """'03:00-05:10' in local time, with the date when it is not today.

    Never raises: a log line must not be what stops the re-checks.
    """
    try:
        s = datetime.datetime.fromtimestamp(start)
        e = datetime.datetime.fromtimestamp(end)
        if s.date() == e.date():
            span = f'{s:%H:%M}-{e:%H:%M}'
            if s.date() == datetime.datetime.fromtimestamp(now).date():
                return span
            return f'{s:%Y-%m-%d} {span}'
        return f'{s:%Y-%m-%d %H:%M} - {e:%Y-%m-%d %H:%M}'
    except (OverflowError, OSError, ValueError, TypeError):
        return f'{start!r}-{end!r}'


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
        self._heartbeat = time.monotonic()
        self._watchdog_thread = None
        self._hang_threshold = WATCHDOG_HANG_THRESHOLD
        # Process exit status.  Set to non-zero on any abnormal exit so
        # systemd's Restart=on-failure/always actually sees a failure - a
        # clean exit(0) here silently ends collection until someone notices.
        self._exit_code = 0
        # Throttling of the expensive per-sensor "deep" gap scan.
        # _fetch_high_water is the stop time of the last completed cycle;
        # _last_deep_scan is a monotonic timestamp of the last deep scan.
        self._fetch_high_water = None
        self._last_deep_scan = None
        # Per-sensor return tracking, persisted across restarts:
        #   {sensor_id: {'returned_at': <epoch>, 'gap_from': <iso8601>}}
        # A sensor that has started reporting again is recorded here and
        # back-filled only after sensor_return_grace has elapsed.
        self._sensor_state = {}
        # Filled in by the deep scan, consumed by _handle_sensor_returns().
        self._sensor_last_stored = {}
        self._collector_high_water = None
        # Windows whose fetch failed outright.  They are retried on later
        # cycles; until they succeed the high-water mark must not advance
        # past them or the hole becomes invisible to gap detection.
        self._failed_windows = []
        # End of the newest collection known to have landed completely
        # (persisted copy of _fetch_high_water).  Only consulted when the
        # backends cannot be asked for their newest point - see
        # _known_collected_until().
        self._collected_until = None
        # (stored sensor_id, sensor_name) -> full API id, refreshed every
        # cycle, for backends with SENSOR_UID enabled.
        self._sensor_uids = {}
        self._reported_collisions = set()
        self._logged_id_map = False
        self._warned_backlog = False
        # Gap re-checks (RECHECK_GAPS), daemon mode only: holes found in the
        # fetched sample streams, re-read on a schedule because gateways
        # upload missed samples late.  See _note_fetched()/_recheck_gaps().
        # Parsed before anything is fetched: an invalid value ends the
        # process right here.
        self._recheck_offsets, self._recheck_min_gap = self._recheck_settings()
        self._recheck_on = bool(self._recheck_offsets) and bool(args.daemon)
        # Persisted (state.json, key 'rechecks') only while the feature is
        # on: the newest observed time per sensor, and the holes.
        self._recheck_last_seen = {}
        self._recheck_holes = []
        # In memory only: the current stretch of live polling, what the
        # windows of the current cycle have read, the back-off of the
        # re-reads read together and that of the cells read alone (failed
        # calls in a row, and until when - monotonic; see _recheck_gaps),
        # whether the next read alone after the others goes to the kept
        # kind (see _recheck_pick), whether re-reads wait for a backend, and
        # once-only warnings.
        self._live_since = None
        self._live_poll_at = None
        self._cycle_cover = []
        self._recheck_fail_streak = 0
        self._recheck_next_at = None
        self._recheck_alone_streak = 0
        self._recheck_alone_next_at = None
        self._recheck_kept_next = True
        self._recheck_writers_waiting = False
        self._recheck_warned = set()
        self._recheck_cap_warned = {}
        self._recheck_total_warned = None
        self._note_warned = False
        self._state_path = self._resolve_state_path()
        self._load_state()
        self._setup_signals()
        self._report_writer_settings()

    def _report_writer_settings(self):
        """Point out per-backend settings that do not do what they look like."""
        names = [(type(w).__name__, w.measurement_name) for w in self.writers]
        if len({name for _, name in names}) > 1:
            # Records are built once and written to every backend, so they
            # all get the first backend's name.  The gap check therefore
            # queries that name on every backend too (it used to query each
            # backend's own name, found nothing, and silently stopped
            # back-filling there).  Writing each backend under its own name
            # instead would move existing data to a new measurement.
            logger.warning(
                "MEASUREMENT_NAME differs between backends (%s); all backends "
                "are written as '%s', the first backend's name",
                ', '.join(f'{k}={v}' for k, v in names),
                self._get_measurement_name())
        for w in self.writers:
            if getattr(w, 'sensor_uid', False):
                logger.info(
                    "%s: SENSOR_UID is on - every point also stores the full "
                    "SensorPush id as tag sensor_uid (sensor_id is unchanged)",
                    type(w).__name__)

    # -- persistent state ---------------------------------------------------

    def _resolve_state_path(self):
        """Pick a writable location for the sensor-return state file.

        Prefers systemd's StateDirectory=, which stays writable under
        ProtectSystem=strict.  Falls back to the XDG state dir for manual
        runs.  Returns None if nothing is writable - the daemon then runs
        with in-memory state only, which just means a restart re-arms the
        grace period rather than losing data.
        """
        candidates = []
        sd_state = os.environ.get('STATE_DIRECTORY')
        if sd_state:
            candidates.append(Path(sd_state.split(':')[0]))
        xdg = os.environ.get('XDG_STATE_HOME')
        candidates.append(Path(xdg) / 'sensorpushd' if xdg
                          else Path.home() / '.local' / 'state' / 'sensorpushd')
        for base in candidates:
            try:
                base.mkdir(parents=True, exist_ok=True)
                probe = base / '.write-test'
                probe.touch()
                probe.unlink()
                return base / 'state.json'
            except Exception:
                continue
        logger.warning("No writable state directory found; sensor-return "
                       "tracking will not survive a restart")
        return None

    def _load_state(self):
        # Written by this release (which always stores the key, null until
        # known)?  Only state from an older release may adopt its marker.
        current_format = False
        if self._state_path and self._state_path.is_file():
            try:
                with open(self._state_path) as f:
                    data = json.load(f)
                self._sensor_state = data.get('sensors', {})
                self._failed_windows = data.get('failed_windows', [])
                current_format = 'collected_until' in data
                if data.get('collected_until'):
                    try:
                        ts = datetime.datetime.fromisoformat(
                            data['collected_until'])
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=datetime.timezone.utc)
                        self._collected_until = ts
                    except (TypeError, ValueError):
                        logger.warning("Ignoring unreadable collected_until "
                                       "%r in %s", data['collected_until'],
                                       self._state_path)
                if self._sensor_state or self._failed_windows:
                    logger.info("Restored state: %d sensor(s) pending "
                                "back-fill, %d failed window(s) to retry",
                                len(self._sensor_state),
                                len(self._failed_windows))
            except Exception as e:
                logger.warning("Could not read state file %s: %s",
                               self._state_path, e)
        if self._recheck_on:
            self._load_recheck_state()
        if not current_format:
            self._adopt_last_success_marker()

    def _adopt_last_success_marker(self):
        """First start after upgrading: seed collected_until, once.

        Older releases wrote last-success at the end of a cycle that had
        stored data, so their marker is the best "collected up to" point
        available.  The state file is rewritten in this release's format
        right away - with the adopted value, or null - because from now on
        last-success is refreshed after every window that lands, also
        halfway through a long back-fill, and must never be adopted again.
        """
        marker = self._read_last_success_marker()
        if marker is not None:
            self._collected_until = marker
            logger.info("Taking the last-success marker (%s) as the end of "
                        "the last completed collection", marker.isoformat())
        self._save_state()

    def _write_last_success(self):
        """Publish the last time data actually reached a backend.

        Process supervision cannot see the failure mode that matters most
        - a daemon that is alive but storing nothing - so this timestamp
        is what the freshness check alerts on.  Only written when a write
        genuinely succeeded, never merely when a cycle did not raise - and
        never in dry-run mode, where nothing is written at all.
        """
        if not self._state_path or self.args.dryrun:
            return
        try:
            path = self._state_path.parent / 'last-success'
            tmp = path.with_suffix('.tmp')
            tmp.write_text(self._last_successful_time.isoformat() + '\n')
            os.replace(tmp, path)
        except Exception as e:
            logger.debug("Could not write last-success marker: %s", e)

    def _mark_success(self):
        """Note that data just reached a backend.

        Called after every window that landed, not once per cycle: a long
        back-fill can take hours, and data is flowing the whole time, so
        the freshness check must not alert during it.
        """
        self._last_successful_time = datetime.datetime.now(
            tz=datetime.timezone.utc)
        self._write_last_success()

    def _read_last_success_marker(self):
        if not self._state_path:
            return None
        try:
            text = (self._state_path.parent / 'last-success').read_text()
            ts = datetime.datetime.fromisoformat(text.strip())
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.debug("Could not read last-success marker: %s", e)
            return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return ts

    def _known_collected_until(self):
        """End of the newest collection known to have landed completely.

        Only a fallback for when a backend cannot be asked for its newest
        stored point.  Within this process that is the incremental
        high-water mark; after a restart it is the copy persisted in
        state.json (seeded once from an older release's last-success
        marker, see _adopt_last_success_marker).  None if neither exists.

        The mark covers the collector as a whole - it advances once a
        cycle's windows reached at least one backend - so for a backend
        that was rejecting writes while another one accepted them it can be
        too recent.  That backend's own gap is found once it accepts writes
        and answers lookups again.
        """
        if self._fetch_high_water is not None:
            return self._fetch_high_water
        return self._collected_until

    def _save_state(self):
        """Persist state atomically so a crash mid-write cannot corrupt it.

        Never in dry-run mode: a dry run must leave pending back-fills,
        failed windows and the collection mark exactly as it found them.
        """
        if not self._state_path or self.args.dryrun:
            return
        payload = {
            'sensors': self._sensor_state,
            'failed_windows': self._failed_windows,
            # Always present (null until known): marks the file as written
            # by a release that refreshes last-success per window.
            'collected_until': (self._collected_until.isoformat()
                                if self._collected_until is not None else None),
        }
        # Only while the feature is on: off, the file is exactly what it was
        # without it (and an older release drops the key on its next save).
        if self._recheck_on:
            try:
                payload['rechecks'] = self._recheck_payload()
            except Exception as e:
                logger.warning("Could not save the gap re-checks: %s", e)
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(dir=str(self._state_path.parent),
                                       prefix='.state-', suffix='.tmp')
            with os.fdopen(fd, 'w') as f:
                json.dump(payload, f)
            os.replace(tmp, self._state_path)
        except Exception as e:
            logger.warning("Could not write state file %s: %s",
                           self._state_path, e)
            # Saves happen every cycle; on a full disk, leftover temp files
            # would pile up.
            if tmp is not None:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def _setup_signals(self):
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGHUP, self._handle_signal)

    def _handle_signal(self, signum, frame):
        if signum in (signal.SIGTERM, signal.SIGINT):
            logger.info("Received %s, shutting down gracefully...",
                        signal.Signals(signum).name)
            sd_notify('STOPPING=1')
            self.running = False
        elif signum == signal.SIGHUP:
            logger.info("Received SIGHUP, will reload configuration on next cycle")

    def _touch(self):
        """Record forward progress for the watchdog heartbeat.

        A plain float assignment is atomic under the GIL, so no lock is
        needed.  Called whenever the daemon does meaningful work (each
        cycle, each fetch iteration, each second of sleep) so the
        background feeder knows the main loop is alive.
        """
        self._heartbeat = time.monotonic()

    def _start_watchdog(self):
        """Feed the systemd watchdog from a background thread.

        The feeder pings systemd only while the main loop keeps making
        progress (see _touch).  This lets a long but healthy operation -
        e.g. backfilling days or weeks of data for a sensor that has been
        offline - run to completion without being killed, while a process
        that is genuinely stuck stops being pinged and gets restarted by
        systemd.  Does nothing when not running under a watchdog.
        """
        wd_usec = os.environ.get('WATCHDOG_USEC')
        if not wd_usec:
            logger.debug("No systemd watchdog configured (WATCHDOG_USEC unset)")
            return
        try:
            wd_sec = int(wd_usec) / 1_000_000.0
        except ValueError:
            logger.warning("Invalid WATCHDOG_USEC=%r, watchdog feeder disabled",
                           wd_usec)
            return
        ping_interval = max(1.0, wd_sec / 3.0)
        # Derive the hang threshold from WatchdogSec so tightening the unit
        # tightens real hang detection.  Floored so a legitimate long step
        # (an API call plus its retries) is never mistaken for a hang.
        self._hang_threshold = max(WATCHDOG_HANG_THRESHOLD, wd_sec * 0.75)

        def _loop():
            while self.running:
                stale = time.monotonic() - self._heartbeat
                if stale < self._hang_threshold:
                    sd_notify('WATCHDOG=1')
                else:
                    logger.error(
                        "Watchdog heartbeat stale for %.0fs (>%.0fs) - not "
                        "pinging systemd; the daemon will be restarted",
                        stale, self._hang_threshold)
                time.sleep(ping_interval)

        self._watchdog_thread = threading.Thread(
            target=_loop, name="watchdog-feeder", daemon=True)
        self._watchdog_thread.start()
        logger.info(
            "Watchdog feeder started (timeout=%.0fs, ping every %.0fs, "
            "hang threshold=%.0fs)",
            wd_sec, ping_interval, self._hang_threshold)

    def _get_measurement_name(self):
        """Get the measurement name from the first active backend."""
        if self.writers:
            return self.writers[0].measurement_name
        return 'SensorPush'

    def _interruptible_sleep(self, seconds):
        """Sleep that can be interrupted by signals.

        Updates the watchdog heartbeat every second so the background
        feeder keeps the systemd timer satisfied during idle periods
        (between cycles and during retry backoffs).
        """
        for i in range(int(seconds)):
            if not self.running:
                return
            self._touch()
            time.sleep(1)

    def run(self):
        """Main daemon loop.

        Returns the process exit status.  A collection failure never ends
        the daemon by default: the upstream API can be down for hours, and
        a collector that gives up permanently turns a transient outage into
        an indefinite one.  Instead the retry interval backs off
        exponentially (capped) and the failure is escalated to CRITICAL and
        reported to systemd once it stops looking transient.
        """
        interval = self.args.interval or self.config['daemon']['interval']
        max_failures = int(self.config['daemon'].get(
            'max_consecutive_failures', 0) or 0)
        logger.info("Starting SensorPush daemon (interval=%ds, "
                    "give up after=%s)", interval,
                    max_failures if max_failures > 0 else 'never')

        # Connect to all backends (with retry)
        self._connect_writers()

        sd_notify('READY=1')
        sd_status('Starting first collection cycle')
        self._start_watchdog()

        consecutive_failures = 0

        while self.running:
            self._touch()
            sleep_for = interval
            try:
                self._collect_cycle()
                consecutive_failures = 0
                sd_status('OK: last successful cycle %s' %
                          self._last_successful_time)
            except Exception as e:
                consecutive_failures += 1
                # Exponential backoff, capped, so a multi-hour upstream
                # outage costs a handful of requests rather than thousands.
                sleep_for = min(interval * 2 ** min(consecutive_failures - 1, 8),
                                MAX_CYCLE_BACKOFF)
                log = (logger.critical if consecutive_failures >= CYCLE_ALERT_AFTER
                       else logger.error)
                log("Collection cycle failed (%d consecutive), retrying in "
                    "%ds: %s", consecutive_failures, sleep_for, e)
                sd_status('DEGRADED: %d consecutive failures: %s'
                          % (consecutive_failures, e))
                if max_failures > 0 and consecutive_failures >= max_failures:
                    logger.critical(
                        "Too many consecutive failures (%d), exiting",
                        consecutive_failures)
                    sd_status('FATAL: giving up after %d consecutive failures'
                              % consecutive_failures)
                    self._exit_code = 1
                    self.running = False
                    break

            self._interruptible_sleep(sleep_for)

        self._save_state()
        for w in self.writers:
            try:
                w.close()
            except Exception as e:
                logger.warning("Error closing %s: %s", type(w).__name__, e)
        logger.info("Daemon stopped")
        return self._exit_code

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
        if self._recheck_on:
            # What this cycle's windows have read (see _note_fetched).
            self._cycle_cover = []

        # Authenticate, reusing the cached token while it is still valid.
        # Re-authenticating every cycle multiplied exposure to auth outages
        # and cost ~11x the requests for no benefit.
        self.api._ensure_auth()

        # Fetch and write voltage/RSSI data
        sensors = self.api.get_sensors()
        self._note_sensors(sensors)
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
        deep_scan = False
        if self.args.daemon:
            starttime, stoptime, deep_scan = self._compute_daemon_window(
                mytz, currenttime, sensors)
        else:
            starttime, stoptime = self._compute_oneshot_window(
                mytz, currenttime)

        # Re-drive any window that previously failed outright, before
        # advancing anything - otherwise the hole is never revisited.
        if self.args.daemon:
            self._retry_failed_windows(sensors, measurement_name)

        timelist = build_timelist(starttime, stoptime, self.args.timestep)
        iterations = len(timelist)

        logger.info("Start: %s",
                     datetime.date.strftime(starttime, '%Y-%m-%dT%X%z'))
        logger.info("Stop:  %s",
                     datetime.date.strftime(stoptime, '%Y-%m-%dT%X%z'))
        logger.info("Iterations required: %d", iterations)

        # Fetch and write samples.  The freshness marker is updated after
        # every window that lands (see _mark_success).
        all_ok = True
        state_dirty = False
        for i, window in enumerate(timelist, 1):
            if not self.running:
                logger.info("Shutdown requested, stopping collection")
                all_ok = False
                break
            if not self._fetch_and_write_window(
                    window, i, iterations, sensors, measurement_name):
                all_ok = False
                self._record_failed_window(window)
        else:
            # Loop ran to completion (not aborted by shutdown).  Only
            # advance the high-water mark if every window actually landed;
            # advancing past a failed window hides it from gap detection,
            # which only ever looks at the newest stored point.
            if self.args.daemon and all_ok:
                self._fetch_high_water = stoptime
                if not self.args.dryrun:
                    self._collected_until = stoptime
                    state_dirty = True

        # Sensors lagging behind the rest are handled separately, and only
        # on deep-scan cycles to keep the probe traffic bounded.
        if self.args.daemon and deep_scan:
            self._handle_sensor_returns(sensors, measurement_name, currenttime)
            state_dirty = True

        # Re-read the recorded gaps that are due.  Only after a main fetch
        # that landed completely - with the API or every backend down they
        # would fail as well - and isolated: an error here costs at most the
        # re-checks, never the collection.
        if self._recheck_on:
            if self.running and all_ok:
                try:
                    self._recheck_gaps(sensors, measurement_name)
                except Exception:
                    logger.error("Gap re-check failed; collection is not "
                                 "affected", exc_info=True)
            # Every fetch updates what the re-checks know (_note_fetched).
            state_dirty = True
            self._note_warned = False

        if state_dirty:
            self._save_state()

    def _note_sensors(self, sensors):
        """Per-cycle bookkeeping for the account's current sensor list.

        Refreshes the sensor_uid lookup, reports sensors whose stored
        sensor_id would collide (never changes what is stored), and once
        per process logs which sensor_id each sensor is stored under.
        """
        by_tag = {}
        by_record = {}
        for sid in sensors:
            tag = sensor_id_tag(sid)
            by_tag.setdefault(tag, []).append(str(sid))
            # Records carry sensor_id and sensor_name, so that pair finds
            # the full id - also for two sensors whose sensor_id collides.
            key = (tag, str(sensors[sid].get('name', '')))
            by_record.setdefault(key, []).append(str(sid))
        # Only a pair shared by several ids (same stored id *and* same
        # name) cannot be attributed; it gets no sensor_uid, not a wrong one.
        self._sensor_uids = {key: ids[0] for key, ids in by_record.items()
                             if len(ids) == 1}
        for tag, ids in sorted(by_tag.items()):
            key = (tag, tuple(sorted(ids)))
            if len(ids) < 2 or key in self._reported_collisions:
                continue
            self._reported_collisions.add(key)
            logger.error(
                "Sensors %s are all stored as sensor_id=%s. Queries that "
                "filter on sensor_id, and the daemon's gap check, cannot tell "
                "them apart; sensor_name still does. SENSOR_UID=True stores "
                "the full ids as well. Nothing stored has been changed.",
                ', '.join(sorted(ids)), tag)
        if not self._logged_id_map and self.args.daemon:
            self._logged_id_map = True
            for sid in sorted(sensors, key=lambda s: str(sensors[s].get('name', s))):
                logger.info("Sensor '%s': API id %s is stored as sensor_id=%s",
                            sensors[sid].get('name', sid), sid,
                            sensor_id_tag(sid))

    def _gap_lookback_min(self):
        """How far back gap-detection queries should search.

        Must be at least as long as the gap being recovered, or the
        lagging sensor looks like it has no data at all and back-filling
        is silently skipped.  Derived from max_backfill (plus a wide
        margin) so the two settings cannot disagree; 0 means unbounded.
        """
        cfg = self.config['daemon'].get('gap_lookback', '0')
        if cfg and cfg != '0':
            return parse_backlog(cfg)
        max_backfill_str = self.config['daemon']['max_backfill']
        if max_backfill_str and max_backfill_str != '0':
            return parse_backlog(max_backfill_str) * 2
        return 0

    def _compute_daemon_window(self, mytz, currenttime, sensors):
        """Compute time window for daemon mode.

        Returns ``(starttime, stoptime, deep_scan_ran)``.

        Two modes:

        * Deep scan - queries every sensor on every backend to find how
          far behind the *collector* is, i.e. the newest point each
          backend holds.  Taking the newest (not the oldest) is what keeps
          a single dead sensor from dragging the window across the entire
          history on every scan; sensors lagging behind the pack are dealt
          with individually by _handle_sensor_returns().
        * Incremental poll - between deep scans, simply continue from the
          high-water mark of the last completed cycle.  This keeps online
          sensors current cheaply.
        """
        poll_backlog_min = self._backlog_min(
            self.config['daemon']['poll_backlog'], 'POLL_BACKLOG')
        max_backfill_str = self.config['daemon']['max_backfill']
        max_backfill_min = parse_backlog(max_backfill_str) if max_backfill_str != '0' else 0
        deep_scan_str = self.config['daemon'].get('deep_scan_interval', '1h')
        deep_scan_min = parse_backlog(deep_scan_str) if deep_scan_str != '0' else 0
        lookback_min = self._gap_lookback_min()

        # Decide whether a deep scan is due.  Always deep-scan on the first
        # cycle (no high-water mark yet) and when throttling is disabled.
        now_mono = time.monotonic()
        deep_due = (
            self._fetch_high_water is None
            or self._last_deep_scan is None
            or deep_scan_min <= 0
            or (now_mono - self._last_deep_scan) >= deep_scan_min * 60
        )

        if not deep_due:
            # Incremental poll from the last completed cycle, with a small
            # overlap, but never a shorter window than poll_backlog.
            overlap = datetime.timedelta(minutes=2)
            fast_start = self._fetch_high_water - overlap
            backlog_start = currenttime - datetime.timedelta(
                minutes=poll_backlog_min)
            starttime = min(fast_start, backlog_start)
            mins_to_deep = deep_scan_min - (now_mono - self._last_deep_scan) / 60
            logger.info(
                "Incremental poll from %s (deep scan in ~%.0f min)",
                starttime.strftime('%Y-%m-%dT%X%z'), max(0, mins_to_deep))
            return starttime, currenttime, False

        # --- deep scan ---
        self._last_deep_scan = now_mono
        logger.info("Running deep gap scan (deep_scan_interval=%s)",
                    deep_scan_str)

        # The sensors to check: the API ids, limited to --sensorlist when
        # one is given (otherwise an excluded sensor with old data would be
        # tracked as stale and back-filled anyway).
        sensor_ids = list(sensors.keys())
        if self.args.sensorlist:
            wanted = {str(s) for s in self.args.sensorlist}
            sensor_ids = [sid for sid in sensor_ids if str(sid) in wanted]
        # Look up the measurement the records are actually written to -
        # the first backend's name, on every backend.
        measurement_name = self._get_measurement_name()

        # per_sensor[sid]  = oldest "last seen" across backends, i.e. the
        #                    point from which that sensor still needs data
        # per_writer[name] = newest "last seen" on that backend, i.e. how
        #                    current the backend as a whole is
        per_sensor = {}
        per_writer = {}
        # Backends whose newest point is unknown this scan: wname -> reason.
        # 'unreadable' = asked, but every lookup failed; 'skipped' = not
        # asked because it is disconnected or its last write failed.
        unreadable = {}
        skipped = {}
        fallback = self._known_collected_until()

        for writer in self.writers:
            wname = type(writer).__name__
            if not writer.connected or writer._consecutive_failures > 0:
                skipped[wname] = (
                    'not connected' if not writer.connected else
                    f'{writer._consecutive_failures} consecutive write failures')
                logger.debug("Skipping gap check on %s (%s)", wname,
                             skipped[wname])
                continue
            failed = 0
            last_error = None
            for sid in sensor_ids:
                if not self.running:
                    break
                self._touch()
                # Query by the value the sensor is stored under - the same
                # sensor_id_tag() the record builders use - never by the
                # raw API id.
                sid_tag = sensor_id_tag(sid)
                try:
                    ts = writer.get_last_timestamp(
                        measurement_name, sensor_id=sid_tag,
                        lookback_min=lookback_min)
                except Exception as e:
                    failed += 1
                    last_error = e
                    logger.debug(
                        "Could not query last timestamp for sensor %s "
                        "from %s: %s", sid, wname, e)
                    continue
                if ts is None:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                if sid not in per_sensor or ts < per_sensor[sid]:
                    per_sensor[sid] = ts
                if wname not in per_writer or ts > per_writer[wname]:
                    per_writer[wname] = ts
            if failed and wname not in per_writer:
                unreadable[wname] = last_error
            elif failed:
                logger.warning(
                    "Could not query the newest stored point of %d of %d "
                    "sensor(s) on %s (last error: %s); they sit out this gap "
                    "check", failed, len(sensor_ids), wname, last_error)
                # If only stale sensors answered, their old points must not
                # drag the window back over data known to be collected.
                if fallback is not None and fallback > per_writer[wname]:
                    per_writer[wname] = fallback

        # A backend whose newest point is unknown must not count as empty:
        # that would fetch only the poll backlog, and once the next write
        # lands the outage before this start can never be found again.
        # Assume instead that it holds everything up to the last collection
        # known to have completed.
        when = (fallback.astimezone(mytz).strftime('%Y-%m-%dT%X%z')
                if fallback is not None else None)
        for wname, err in unreadable.items():
            if fallback is not None:
                logger.warning(
                    "Could not read the newest stored point from %s (%s); "
                    "resuming from %s, the end of the last collection known "
                    "to have completed", wname, err, when)
                per_writer[wname] = fallback
            else:
                logger.warning(
                    "Could not read the newest stored point from %s (%s), "
                    "and no completed collection is on record; fetching only "
                    "the poll backlog. If the daemon was down before this "
                    "start, back-fill that time with a one-shot run (-s/-p).",
                    wname, err)
        for wname, reason in skipped.items():
            if fallback is not None:
                logger.info("Not asking %s for its newest point (%s); assuming "
                            "it holds everything up to %s", wname, reason, when)
                per_writer[wname] = fallback

        # Remember for _handle_sensor_returns(); local tz for logging.
        self._sensor_last_stored = {
            sid: ts.astimezone(mytz) for sid, ts in per_sensor.items()}

        # The collector is only as current as its most-behind backend.
        collector_ts = min(per_writer.values()) if per_writer else None
        self._collector_high_water = (collector_ts.astimezone(mytz)
                                      if collector_ts else None)
        # That is also how far collection is known to be complete.  Persist
        # it now, before a possibly long back-fill starts: if the process
        # dies halfway and the next start cannot read the backends, this is
        # where it must resume - not wherever the back-fill had got to.
        # (Not after a shutdown interrupted the scan: then only some backends
        # were asked.)
        if (self.running and collector_ts is not None
                and collector_ts != self._collected_until):
            self._collected_until = collector_ts
            self._save_state()

        if collector_ts:
            collector_local = collector_ts.astimezone(mytz)
            gap_minutes = (currenttime - collector_local).total_seconds() / 60
            if gap_minutes > poll_backlog_min:
                behind = min(per_writer, key=per_writer.get)
                logger.info(
                    "Collector gap: %s last stored data at %s (%.0f min ago), "
                    "fetching with 1h safety margin",
                    behind, collector_local.strftime('%Y-%m-%dT%X%z'),
                    gap_minutes)
                # Go back 1 hour before the last stored point as safety margin
                starttime = collector_local - datetime.timedelta(hours=1)
            else:
                starttime = currenttime - datetime.timedelta(
                    minutes=poll_backlog_min)
        else:
            logger.info("No existing data found, using poll backlog of %d min",
                        poll_backlog_min)
            starttime = currenttime - datetime.timedelta(
                minutes=poll_backlog_min)

        # Cap how far back we go if max_backfill is set
        if max_backfill_min > 0:
            earliest_allowed = currenttime - datetime.timedelta(
                minutes=max_backfill_min)
            if starttime < earliest_allowed:
                logger.warning(
                    "Capping backfill start from %s to %s (max_backfill=%s) - "
                    "data older than the cap will not be recovered",
                    starttime.strftime('%Y-%m-%dT%X%z'),
                    earliest_allowed.strftime('%Y-%m-%dT%X%z'),
                    max_backfill_str)
                starttime = earliest_allowed

        return starttime, currenttime, True

    def _handle_sensor_returns(self, sensors, measurement_name, currenttime):
        """Back-fill sensors that have started reporting again.

        A sensor can be out of range for weeks.  Its gateway only uploads
        the buffered backlog once the sensor is back in range, and that
        upload is not instant - so back-filling the moment the first new
        sample appears would fetch a window the cloud has not finished
        populating.  Instead: notice the return, wait
        ``sensor_return_grace``, then fetch the sensor's whole gap once.

        A sensor that is simply dead (flat battery) never produces new
        data, so it costs exactly one cheap probe per deep scan and never
        triggers a back-fill - which is what stops one dead sensor from
        forcing a full-history refetch of everything, every hour.

        The gap is recorded as soon as a sensor falls behind, *before* it
        returns.  Gap detection reads the newest stored point, so the
        moment a sensor resumes, the ordinary incremental poll stores a
        fresh sample and that point jumps to the present - the sensor
        stops looking stale and the hole behind it becomes invisible.
        Remembering the gap up front is what keeps a returning sensor's
        backlog recoverable.
        """
        per_sensor = getattr(self, '_sensor_last_stored', {})
        collector_hw = getattr(self, '_collector_high_water', None)
        if not per_sensor or collector_hw is None:
            return

        stale_str = self.config['daemon'].get('sensor_stale_after', '6h')
        stale_after_min = parse_backlog(stale_str)
        grace_str = self.config['daemon'].get('sensor_return_grace', '1h')
        grace_sec = parse_backlog(grace_str) * 60 if grace_str != '0' else 0
        now = time.time()

        for sid, last_ts in per_sensor.items():
            if not self.running:
                return
            name = sensors.get(sid, {}).get('name', sid)
            lag_min = (collector_hw - last_ts).total_seconds() / 60
            key = str(sid)

            state = self._sensor_state.get(key)

            if state is None:
                if lag_min <= stale_after_min:
                    continue  # keeping up with the pack - nothing to do
                # First time this sensor is seen behind.  Record the gap
                # now; once it resumes there is no way to find it again.
                self._sensor_state[key] = {
                    'gap_from': last_ts.isoformat(),
                    'stale_since': now,
                }
                logger.info(
                    "Sensor '%s' has fallen behind (no data since %s, %.1f h); "
                    "recorded its gap to back-fill when it returns", name,
                    last_ts.strftime('%Y-%m-%dT%X%z'), lag_min / 60)
                continue

            # A gap is on file for this sensor.  From here the current lag
            # is irrelevant - the sensor may already look current again
            # while the hole behind it is still unfilled.
            gap_from = datetime.datetime.fromisoformat(state['gap_from'])

            if not self._sensor_has_new_data(sid, gap_from, currenttime):
                if state.pop('returned_at', None):
                    logger.info("Sensor '%s' stopped reporting again before "
                                "its back-fill ran; rearming", name)
                else:
                    logger.info(
                        "Sensor '%s' still has no data since %s; nothing to "
                        "back-fill yet", name,
                        gap_from.strftime('%Y-%m-%dT%X%z'))
                continue

            if 'returned_at' not in state:
                state['returned_at'] = now
                logger.info(
                    "Sensor '%s' is reporting again (gap since %s); waiting %s "
                    "for its gateway to finish uploading before back-filling",
                    name, gap_from.strftime('%Y-%m-%dT%X%z'), grace_str)
                continue

            waited = now - float(state['returned_at'])
            # Small tolerance: probes happen once per deep scan, so an exact
            # comparison would routinely miss by a second and push the
            # back-fill out by a whole extra scan interval.
            if waited < grace_sec - 60:
                logger.info(
                    "Sensor '%s' back-fill pending (%.0f of %.0f min settle "
                    "time elapsed)", name, waited / 60, grace_sec / 60)
                continue

            logger.info("Sensor '%s' settled for %.0f min; back-filling from %s",
                        name, waited / 60, gap_from.strftime('%Y-%m-%dT%X%z'))
            if self._backfill_sensor(sid, gap_from, currenttime, sensors,
                                     measurement_name):
                self._sensor_state.pop(key, None)
                logger.info("Sensor '%s' back-fill complete", name)
            else:
                logger.warning(
                    "Sensor '%s' back-fill incomplete; will retry on the next "
                    "deep scan", name)

    def _sensor_has_new_data(self, sid, last_ts, currenttime):
        """Cheap probe: does the cloud hold anything newer for this sensor?

        The sensors endpoint carries no last-seen timestamp (``active``
        and ``rssi`` stay stale on a dead sensor), so presence has to be
        established with a one-sample query.  Starts just after the stored
        point so an inclusive range cannot re-report it as new.
        """
        probe_start = last_ts + datetime.timedelta(seconds=1)
        if probe_start >= currenttime:
            return False
        try:
            self._touch()
            samples = self.api.get_samples(
                datetime.date.strftime(probe_start, '%Y-%m-%dT%X%z'),
                datetime.date.strftime(currenttime, '%Y-%m-%dT%X%z'),
                measures=['temperature'], limit=1, sensors=[str(sid)])
            return int(samples.get('total_samples', 0)) > 0
        except Exception as e:
            logger.warning("Could not probe sensor %s for new data: %s", sid, e)
            return False

    def _backfill_sensor(self, sid, last_ts, currenttime, sensors,
                         measurement_name):
        """Fetch one sensor's whole gap. Returns True if every window landed."""
        start = last_ts - datetime.timedelta(hours=1)
        max_backfill_str = self.config['daemon']['max_backfill']
        if max_backfill_str and max_backfill_str != '0':
            earliest = currenttime - datetime.timedelta(
                minutes=parse_backlog(max_backfill_str))
            if start < earliest:
                logger.warning(
                    "Capping sensor back-fill start from %s to %s "
                    "(max_backfill=%s)", start.strftime('%Y-%m-%dT%X%z'),
                    earliest.strftime('%Y-%m-%dT%X%z'), max_backfill_str)
                start = earliest
        timelist = build_timelist(start, currenttime, self.args.timestep)
        iterations = len(timelist)
        logger.info("Sensor back-fill: %d iteration(s) from %s",
                    iterations, start.strftime('%Y-%m-%dT%X%z'))
        ok = True
        for i, window in enumerate(timelist, 1):
            if not self.running:
                return False
            if not self._fetch_and_write_window(
                    window, i, iterations, sensors, measurement_name,
                    sensor_filter=[str(sid)]):
                ok = False
        return ok

    def _record_failed_window(self, window):
        """Remember a window that could not be fetched, for a later retry."""
        entry = list(window)
        if entry not in self._failed_windows:
            self._failed_windows.append(entry)
            logger.error("Recorded failed window %s -> %s for retry "
                         "(%d pending)", entry[0], entry[1],
                         len(self._failed_windows))
            self._save_state()

    def _retry_failed_windows(self, sensors, measurement_name):
        """Re-drive previously failed windows; drop them once they land."""
        if not self._failed_windows:
            return
        pending = list(self._failed_windows)
        logger.info("Retrying %d previously failed window(s)", len(pending))
        for i, window in enumerate(pending, 1):
            if not self.running:
                return
            if self._fetch_and_write_window(
                    window, i, len(pending), sensors, measurement_name):
                try:
                    self._failed_windows.remove(window)
                except ValueError:
                    pass
        self._save_state()

    # -- gap re-checks ------------------------------------------------------

    def _recheck_settings(self):
        """Parse RECHECK_GAPS and RECHECK_MIN_GAP (daemon mode only).

        Returns (offsets in seconds, min gap in seconds); no offsets means
        off, and one-shot mode ignores both keys.  An invalid value ends the
        process with status 2 before anything is fetched or connected:
        running without the re-checks someone configured - or with the ones
        they meant to turn off - would only be noticed much later.
        """
        if not self.args.daemon:
            return [], 0
        daemon_cfg = self.config['daemon']
        try:
            offsets = parse_recheck_gaps(daemon_cfg.get(
                'recheck_gaps', DEFAULT_RECHECK_CFG['recheck_gaps']))
            if not offsets:
                return [], 0
            min_gap = parse_recheck_min_gap(daemon_cfg.get(
                'recheck_min_gap', DEFAULT_RECHECK_CFG['recheck_min_gap']))
        except ValueError as e:
            logger.error("Invalid [DAEMON] setting: %s", e)
            sys.exit(2)
        logger.info("Gap re-checks: gaps over %s are read again %s after they "
                    "close (RECHECK_GAPS = 0 turns this off)",
                    _fmt_offset(min_gap),
                    ', '.join(_fmt_offset(o) for o in offsets))
        return offsets, min_gap

    def _recheck_cell_min(self):
        """Cell length L in minutes for new holes.

        A call reads a cell plus RECHECK_CALL_PAD on either side, so with
        this L a re-read window is as long as every other window (-t).
        """
        return max(RECHECK_CELL_MIN, int(self.args.timestep) - 60)

    def _load_recheck_state(self):
        """Restore the gap re-checks from state.json (key 'rechecks').

        Read separately from the other keys on purpose: nothing wrong here
        may affect the pending back-fills, the failed windows or the
        collection mark.  Malformed entries are dropped one by one, with one
        WARNING for all of them.
        """
        if not self._state_path or not self._state_path.is_file():
            return
        try:
            with open(self._state_path) as f:
                data = json.load(f)
            raw = data.get('rechecks') if isinstance(data, dict) else None
            if raw is None:
                return
            if not isinstance(raw, dict):
                raise ValueError("'rechecks' is not an object")
            now = time.time()
            dropped = 0
            raw_seen = raw.get('last_seen', {})
            if not isinstance(raw_seen, dict):
                dropped += 1
                raw_seen = {}
            last_seen = {}
            for sid, ts in raw_seen.items():
                # One by one: a bad value costs its own entry only.  One far
                # ahead would turn the gap detection off for that sensor.
                if _plausible_time(ts, now):
                    last_seen[str(sid)] = float(ts)
                else:
                    dropped += 1
            raw_holes = raw.get('holes', [])
            if not isinstance(raw_holes, list):
                dropped += 1
                raw_holes = []
            holes = []
            cell = self._recheck_cell_min()
            for entry in raw_holes:
                hole = _valid_hole(entry, now)
                if hole is None:
                    dropped += 1
                    continue
                if hole['cell'] > cell:
                    # -t was lowered since, which is the cure for truncated
                    # answers: read the hole in the new, smaller cells.
                    hole['cell'] = cell
                    hole['counts'] = [0] * _hole_ncells(hole)
                    hole['pos'] = 0
                offs = self._recheck_offsets
                if hole['pos'] > 0 and hole['done'] < len(offs):
                    # state.json does not keep when a pass began; for one
                    # under way before the restart, take its due time.
                    # Unless it is the last offset's pass, it is then not
                    # the last pass (see _last_pass), and the cells it read
                    # before the restart still get the last read.
                    hole['_since'] = hole['closed_at'] + offs[hole['done']]
                holes.append(hole)
            if dropped:
                logger.warning("Dropped %d malformed gap re-check entr%s from "
                               "%s", dropped, 'y' if dropped == 1 else 'ies',
                               self._state_path)
            self._recheck_last_seen = last_seen
            self._recheck_holes = holes
            if holes:
                logger.info("Restored %d gap(s) to re-check", len(holes))
        except Exception as e:
            logger.warning("Could not read the gap re-checks from %s: %s",
                           self._state_path, e)

    def _recheck_payload(self):
        """The 'rechecks' value for state.json: persisted keys only."""
        return {
            'last_seen': dict(self._recheck_last_seen),
            'holes': [{k: (list(h[k]) if k == 'counts' else h.get(k))
                       for k in HOLE_KEYS} for h in self._recheck_holes],
        }

    def _warn_once(self, key, msg, *args):
        """Log a WARNING once per process for each key."""
        if key not in self._recheck_warned:
            self._recheck_warned.add(key)
            logger.warning(msg, *args)

    def _note_fetched_safely(self, samples, sensors, window,
                             sensor_filter=None):
        """_note_fetched(), isolated: an error costs the gap detection only.

        It runs inside _fetch_and_write_window()'s retry loop, after the
        window was handled; an exception escaping from here would fetch and
        write the window again, or record it as failed.  ``sensor_filter``
        is the window's sensor list (None: all sensors).
        """
        try:
            only = ({str(s) for s in sensor_filter} if sensor_filter
                    else None)
            self._note_fetched(samples, sensors, window, only)
        except Exception as e:
            if not self._note_warned:
                self._note_warned = True
                logger.warning("Gap detection failed for window %s (%s); "
                               "collection is not affected", window, e)
            logger.debug("Gap detection error", exc_info=True)

    def _note_fetched(self, samples, sensors, window, only=None):
        """Record the holes in the sample streams of a fetched window.

        Sees every window the daemon fetches anyway - polls, deep-scan and
        collector-gap windows, retried failed windows, the 6-h path's
        back-fills - but not the re-reads.  A sensor's stream is compared
        with the newest sample seen before (its last_seen; on the first
        cycle after an upgrade, the newest point in the database), so only
        what is new counts and fetching a window again finds nothing.
        ``only`` is the set of sensors the window was fetched for (None:
        all of them).

        A jump of more than the threshold is a hole.  HT1 sensors skip
        unchanged readings in their own records (gaps up to 63 min are
        normal), but while the daemon polls continuously the API also
        serves every reading as a live copy, at most ~13 min apart.  So for
        HT1-like types RECHECK_MIN_GAP applies only to a jump that was
        observed live, RECHECK_SETTLED_MIN_GAP otherwise; types that record
        every minute always use RECHECK_MIN_GAP.

        A window can also start after a sensor's newest sample seen so far:
        after a silence of about 45 minutes or more, after the daemon paused
        that long, or with polls more than 30 minutes apart.  What the cloud
        holds in between was then never fetched, and the jump to the
        window's first sample says nothing about it - the API even leaves
        out up to one chunk after the start of a window in the newest hour.
        Such a stretch is recorded whatever its length: as a hole if the
        jump is longer than the threshold, else as a probe, read once to
        fetch what it holds.  A probe is checked against the same threshold
        as the jump: its re-read sees only own records, which in HT1-like
        data not seen live skip up to an hour.
        """
        now = time.time()
        lo = _observed_epoch(window[0])
        stop = _observed_epoch(window[1])
        reaches_present = (stop is not None
                           and stop >= now - RECHECK_PRESENT_WITHIN)
        if reaches_present:
            if (self._live_poll_at is None
                    or now - self._live_poll_at > RECHECK_LIVE_TOLERANCE):
                # A new stretch of live polling.  The API still serves live
                # copies of about the last 30 minutes, so from here on
                # (with a margin) every reading is seen.
                self._live_since = now - RECHECK_LIVE_TOLERANCE
            self._live_poll_at = now
        # From where on this window returns everything the cloud holds.
        covered = None
        if lo is not None:
            covered = (lo + RECHECK_CHUNK
                       if lo > now - RECHECK_CHUNK_SELECT - RECHECK_CHUNK
                       else lo)
        wanted = ({str(s) for s in self.args.sensorlist}
                  if self.args.sensorlist else None)
        min_gap = self._recheck_min_gap
        settled_gap = max(min_gap, RECHECK_SETTLED_MIN_GAP)
        # A sample from the future (a sensor or gateway clock gone wrong)
        # would hold last_seen ahead and hide every gap until then.
        future = now + RECHECK_PRESENT_WITHIN
        probes = []
        for sid, items in (samples.get('sensors') or {}).items():
            if sid not in sensors or (wanted is not None
                                      and str(sid) not in wanted):
                continue
            times = set()
            unparseable = ahead = 0
            for item in items or ():
                t = (_observed_epoch(item.get('observed'))
                     if isinstance(item, dict) else None)
                if t is None:
                    unparseable += 1
                elif t > future:
                    ahead += 1
                else:
                    times.add(t)
            if unparseable:
                self._warn_once(
                    ('observed', sid), "Sensor '%s': %d sample(s) without a "
                    "readable observed time are left out of gap detection "
                    "(reported once)", sensors[sid].get('name', sid),
                    unparseable)
            if ahead:
                self._warn_once(
                    ('future', sid), "Sensor '%s': %d sample(s) observed in "
                    "the future are left out of gap detection (reported "
                    "once)", sensors[sid].get('name', sid), ahead)
            if not times:
                continue
            times = sorted(times)
            prev = self._recheck_last_seen.get(sid)
            if prev is None:
                stored = self._sensor_last_stored.get(sid)
                prev = stored.timestamp() if stored is not None else None
            # The window does not reliably cover the time right after prev,
            # and no earlier window of this cycle read it.  (A sample at or
            # before prev in the answer proves nothing: older than an hour,
            # it is selected by its own time, the chunks after it by theirs.)
            unread = (prev is not None and covered is not None
                      and prev < covered
                      and not self._cycle_covered(sid, prev, covered))
            if prev is None:
                # No baseline at all (a new sensor): its first sample is.
                prev = times[0]
            every_minute = sensors[sid].get('type') in ONE_PER_MINUTE_TYPES
            for t in times:
                if t <= prev:
                    continue
                live = (reaches_present and self._live_since is not None
                        and prev >= self._live_since)
                threshold = min_gap if every_minute or live else settled_gap
                if unread and t - prev <= threshold:
                    # No gap seen, but the stretch was not read: a probe.
                    # Its re-read is checked against this threshold too.
                    # (RECHECK_MIN_GAP for HT1 data not seen live turned
                    # own-record skips into gaps at INTERVALs of 35-59 min
                    # and after a daemon pause of 45-55 min.)
                    self._register_hole(sid, prev, t, sensors, now=now,
                                        probe=threshold)
                    probes.append(sensors[sid].get('name', sid))
                elif t - prev > threshold:
                    self._register_hole(sid, prev, t, sensors, now=now)
                unread = False
                prev = t
            seen = self._recheck_last_seen.get(sid)
            self._recheck_last_seen[sid] = (times[-1] if seen is None
                                            else max(seen, times[-1]))
        if covered is not None and stop is not None:
            self._cycle_cover.append((covered, min(stop, now), only))
        if probes:
            # DEBUG: with an INTERVAL of 40-59 min this is most cycles.  A
            # probe whose re-read finds data logs that at INFO.
            logger.debug(
                "Window %s -> %s may lack what %s recorded right after the "
                "newest sample seen before; reading that stretch again after "
                "%s", window[0], window[1], ', '.join(sorted(probes)),
                _fmt_offset(self._recheck_offsets[0]))

    def _cycle_covered(self, sid, start, end):
        """True if earlier windows of this cycle read [start, end] for sid."""
        reach = start
        for lo, hi, only in sorted(self._cycle_cover, key=lambda c: c[0]):
            if only is not None and sid not in only:
                continue
            if lo > reach:
                break
            reach = max(reach, hi)
            if reach >= end:
                return True
        return False

    @staticmethod
    def _storage_start(start, end, info, cell):
        """The earliest start a hole ending at end can usefully have.

        A start beyond the sensor's on-board storage moves forward by whole
        cells (of ``cell`` minutes), keeping a little more than the storage:
        so the cells of the holes one outage leaves in sensors of different
        types still line up, and one call serves them all.  Cells longer
        than RECHECK_SPAN_SLACK (-t over 1500) keep at most that much more,
        the most a hole restored from state.json may have.
        """
        days = STORAGE_DAYS.get(info.get('type'), STORAGE_DAYS_UNKNOWN)
        earliest = end - (days + 1) * 86400
        if start >= earliest:
            return start
        step = cell * 60
        return max(start + math.floor((earliest - start) / step) * step,
                   earliest - RECHECK_SPAN_SLACK)

    def _register_hole(self, sid, start, end, sensors, now=None, probe=None):
        """Record that sensor sid has no data between start and end.

        start and end are the observed times of the last sample before and
        the first sample after the gap.  ``now`` is its closed_at; holes
        found in one fetch share it, so their re-reads fall due together.
        ``probe`` (seconds) records a stretch a window did not read rather
        than a gap seen (see _note_fetched); probes are not extended, only
        merged by the caps.
        """
        if now is None:
            now = time.time()
        info = sensors.get(sid) or {}
        name = info.get('name', sid)
        cell = self._recheck_cell_min()
        clamped = self._storage_start(start, end, info, cell)
        if clamped > start:
            logger.info(
                "Gap in '%s' since %s: data before %s is beyond the sensor's "
                "on-board storage and is not re-read", name, _fmt_time(start),
                _fmt_time(clamped))
            start = clamped
        own = [h for h in self._recheck_holes if h['sid'] == sid]
        # Only a hole no read has started on can still grow.
        near = [h for h in own if h['done'] == 0 and h['pos'] == 0
                and not h.get('probe') and probe is None
                and h['end'] >= start - RECHECK_MERGE_WITHIN
                and h['start'] <= end]
        if near:
            target = max(near, key=lambda h: h['end'])
            target['end'] = max(target['end'], end)
            target['start'] = self._storage_start(
                min(target['start'], start), target['end'], info, cell)
            target['cell'] = cell
            target['counts'] = [0] * _hole_ncells(target)
            for key in _HOLE_PASS_KEYS:
                target.pop(key, None)
            logger.info("Gap in '%s' extended: no data %s (%s)", name,
                        _fmt_span(target['start'], target['end'], now),
                        _fmt_duration(target['end'] - target['start']))
            return
        hole = {'sid': sid, 'start': start, 'end': end, 'closed_at': now,
                'done': 0, 'pos': 0, 'cell': cell, 'probe': probe}
        hole['counts'] = [0] * _hole_ncells(hole)
        self._recheck_holes.append(hole)
        own.append(hole)
        if len(own) > RECHECK_HOLES_PER_SENSOR:
            self._merge_closest_holes(own, info, cell,
                                      RECHECK_HOLES_PER_SENSOR)
            last = self._recheck_cap_warned.get(sid)
            if last is None or now - last >= 86400:
                self._recheck_cap_warned[sid] = now
                logger.warning(
                    "Sensor '%s' has more than %d gaps to re-check; its "
                    "closest gaps are merged (reported once a day)", name,
                    RECHECK_HOLES_PER_SENSOR)
        if len(self._recheck_holes) > RECHECK_HOLES_MAX:
            self._cap_all_holes(sensors, cell, now)
        if probe is not None:
            logger.debug("Sensor '%s': %s (%s) was not in the fetched window; "
                         "reading it again after %s", name,
                         _fmt_span(start, end, now), _fmt_duration(end - start),
                         _fmt_offset(self._recheck_offsets[0]))
            return
        logger.info("Gap in '%s': no data %s (%s); re-reading it after %s",
                    name, _fmt_span(start, end, now),
                    _fmt_duration(end - start),
                    ', '.join(_fmt_offset(o) for o in self._recheck_offsets))

    def _merge_closest_holes(self, own, info, cell, keep):
        """Merge one sensor's two closest holes until it has ``keep``.

        Nothing is dropped: the merged hole covers both and is read in full
        from its earlier schedule.  Only holes whose union fits in on-board
        storage are merged, so no storage clamp cuts off what either holds.
        Two probes merged stay a probe, checked against the stricter of
        their thresholds; a probe merged with a gap is a gap.  Returns False
        if no pair could be merged before ``own`` was down to ``keep``.
        """
        while len(own) > keep:
            ordered = sorted(own, key=lambda h: (h['start'], h['end']))
            pairs = [(a, b) for a, b in zip(ordered, ordered[1:])
                     if self._storage_start(
                         a['start'], max(a['end'], b['end']), info, cell)
                     == a['start']]
            if not pairs:
                return False
            a, b = min(pairs,
                       key=lambda pair: pair[1]['start'] - pair[0]['end'])
            a['end'] = max(a['end'], b['end'])
            a['closed_at'] = max(a['closed_at'], b['closed_at'])
            a['done'] = min(a['done'], b['done'])
            a['pos'] = 0
            a['cell'] = cell
            a['counts'] = [0] * _hole_ncells(a)
            a['probe'] = (min(a['probe'], b['probe'])
                          if a.get('probe') and b.get('probe') else None)
            for key in _HOLE_PASS_KEYS + ('_errors',):
                a.pop(key, None)
            own[:] = [h for h in own if h is not b]
            self._drop_hole(b)
        return True

    def _cap_all_holes(self, sensors, cell, now):
        """Merge the closest holes of the sensors with the most holes until
        there are at most RECHECK_HOLES_MAX in all (see
        _merge_closest_holes: nothing is dropped)."""
        while len(self._recheck_holes) > RECHECK_HOLES_MAX:
            by_sid = {}
            for h in self._recheck_holes:
                by_sid.setdefault(h['sid'], []).append(h)
            for sid, own in sorted(by_sid.items(), key=lambda kv: -len(kv[1])):
                if self._merge_closest_holes(own, sensors.get(sid) or {},
                                             cell, len(own) - 1):
                    break
            else:
                break       # no pair fits in on-board storage: keep them all
        last = self._recheck_total_warned
        if last is None or now - last >= 86400:
            self._recheck_total_warned = now
            logger.warning(
                "More than %d gaps to re-check in all; the closest gaps of "
                "the sensors with the most are merged (reported once a day)",
                RECHECK_HOLES_MAX)

    def _drop_hole(self, hole):
        """Forget a hole (by identity: two holes can be equal)."""
        self._recheck_holes = [h for h in self._recheck_holes if h is not hole]

    def _recheck_active_ids(self, sensors):
        """The sensors re-reads are for: current ones, within --sensorlist."""
        ids = {str(s) for s in sensors}
        if self.args.sensorlist:
            ids &= {str(s) for s in self.args.sensorlist}
        return ids

    def _recheck_cap(self, now):
        """Cells starting before this are not read (MAX_BACKFILL); or None."""
        max_backfill_str = self.config['daemon']['max_backfill']
        if max_backfill_str and max_backfill_str != '0':
            return now - parse_backlog(max_backfill_str) * 60
        return None

    def _next_cell(self, hole, cap):
        """The hole's next cell to read from pos on, skipping cells older
        than MAX_BACKFILL (they count as read); None if none is left."""
        n = _hole_ncells(hole)
        k = hole['pos']
        while k < n and cap is not None and _hole_cell(hole, k)[0] < cap:
            k += 1
        return k if k < n else None

    def _early_allowance(self, k):
        """How long before offset k a hole may ride along in another's call."""
        offs = self._recheck_offsets
        if k >= len(offs) - 1:
            # The last read keeps the horizon: at most 30 min early.
            return min(RECHECK_EARLY_LAST_MAX, RECHECK_EARLY_SHARE * offs[-1])
        return RECHECK_EARLY_SHARE * offs[k]

    @staticmethod
    def _window_str(ts):
        """An epoch as a query bound, in local time like every other window."""
        mytz = datetime.timezone(datetime.timedelta(hours=local_time_offset()))
        return datetime.date.strftime(
            datetime.datetime.fromtimestamp(ts, tz=mytz), '%Y-%m-%dT%X%z')

    def _recheck_gaps(self, sensors, measurement_name):
        """Re-read the recorded holes that are due, within the cycle budget.

        Each hole is read again RECHECK_GAPS after it closed - one pass per
        offset, cell by cell - and whatever the cloud holds by then is
        written.  The anchor of a call is the most overdue hole; the call
        reads its next cell plus RECHECK_CALL_PAD either side and serves
        every due hole, of any sensor, whose next cell fits in that window,
        plus holes that are almost due.  After an outage of the whole
        collector one call therefore covers every sensor.

        At most RECHECK_CALLS_PER_CYCLE calls and RECHECK_SECONDS_PER_CYCLE
        per cycle, so a long pass takes many ordinary cycles instead of
        stalling one.  Re-reads wait while a backend fails its writes.  A
        failed call advances nothing.  A cell that failed twice in a row is
        read alone - one sensor, one cell - so it cannot fail the others'
        calls (see _recheck_pick); it is skipped for the pass after
        RECHECK_CELL_MAX_FAILS failed calls, or kept on the last pass (see
        _cell_failed_out).  Calls of cells read together and calls of cells
        read alone each have an exponential back-off, and a failed call
        pauses only its own kind - the common back-off pauses all re-reads -
        so the cells read alone, however many, make at most one failed call
        per step.  Apart from the first read alone of a batch that failed
        twice, they hold up no cell read together; among themselves, the
        cells whose read is their cell's last take every other call, unless
        a backend refused their records, and the others go one gap at a
        time (see _recheck_pick).  A truncated answer halves the cells of
        the holes it served; one that is truncated even in the smallest
        cells counts as a failed call.  So does an answer that a backend
        did not take; only _recheck_pick tells the two apart.
        """
        if (self._recheck_next_at is not None
                and time.monotonic() < self._recheck_next_at):
            return
        now = time.time()
        offs = self._recheck_offsets
        active = self._recheck_active_ids(sensors)
        cap = self._recheck_cap(now)
        for hole in list(self._recheck_holes):
            try:
                if hole['done'] >= len(offs):
                    # E.g. after the schedule was shortened.
                    self._finish_hole(hole, sensors, now)
                elif (hole['sid'] not in active
                        and now > hole['closed_at'] + offs[-1] + 86400):
                    logger.info("Dropping the gap of sensor %s (%s): the "
                                "sensor is no longer polled", hole['sid'],
                                _fmt_span(hole['start'], hole['end'], now))
                    self._drop_hole(hole)
            except Exception as exc:
                self._hole_error(hole, sensors, now, exc)
        started = time.monotonic()
        calls = 0
        while calls < RECHECK_CALLS_PER_CYCLE and self.running:
            due, early = self._recheck_candidates(now, active, cap, sensors)
            pick = self._recheck_pick(due, early, now)
            if pick is None:
                break
            alone, c0, limit, batch, kept = pick
            # Only when a call is about to be made: a backend that fails
            # while no gap is due is none of the re-reads' business.  Before
            # every call, not only the first: a failed read alone may have
            # been a backend that just stopped taking writes, and the next
            # call's answer could not be written either.
            if not self._recheck_writers_ready(
                    sum(1 for e in due if not e['hole'].get('probe'))):
                break
            if calls:
                if (time.monotonic() - started + self.args.delay
                        > RECHECK_SECONDS_PER_CYCLE):
                    break
                self._interruptible_sleep(self.args.delay)
                if not self.running:
                    break
            calls += 1
            if kept is not None:
                # A read alone after the others: the kept kind and the
                # others take every other one (see _recheck_pick).  Counted
                # when the call is made, not when it is picked: a pick that
                # the writers gate or the budget stopped got no call.
                self._recheck_kept_next = not kept
            # The window ends RECHECK_CALL_PAD after the last cell it is for,
            # not after the anchor's full cell length: a short gap's call is
            # no larger than it needs to be.  Every cell keeps its pads.
            ws = math.floor(c0 - RECHECK_CALL_PAD)
            we = math.ceil(min(limit, max(e['d'] for e in batch)
                               + RECHECK_CALL_PAD))
            # A copy, taken before the call: the call drops or changes it.
            failed = dict(self._fail_record(batch[0]) or {}) if alone else {}
            result = self._recheck_call(batch, ws, we, now, sensors,
                                        measurement_name, cap)
            if result == 'ok':
                # Any call that works, a read alone too, shows that the API
                # and every backend work: the common streak starts over, so
                # a later failure pauses one step, not as many as failed
                # calls long ago.  A call read together that works says
                # nothing about the cells read alone: their back-off stays.
                self._recheck_fail_streak = 0
                if alone:
                    self._recheck_alone_streak = 0
                    self._recheck_alone_next_at = None
                    self._read_alone_decides(batch[0], failed, due + early,
                                             True)
                continue
            if (result == 'truncated'
                    and self._recheck_truncated(batch, ws, we)):
                # Not a failure of the API or a backend: the holes are read
                # again in smaller cells at once, without a back-off.
                continue
            # A failed call, an answer a backend did not take, or one
            # truncated even in the smallest cells: retrying at once would
            # only fail again.  All of them count alike; the cells only
            # note whether a backend refused the records (see
            # _recheck_pick).
            self._recheck_failed(batch, ws, we, now, sensors, cap, alone,
                                 result == 'unwritten')
            if alone:
                # Says nothing about the cells read together: they go on.
                self._read_alone_decides(batch[0], failed, due + early, False)
                self._recheck_alone_streak += 1
                pause = self._recheck_pause(self._recheck_alone_streak)
                self._recheck_alone_next_at = time.monotonic() + pause
                logger.debug("Gap re-reads of cells read alone pause for "
                             "%.0f min after %d failed call(s) in a row",
                             pause / 60, self._recheck_alone_streak)
                continue
            self._recheck_fail_streak += 1
            pause = self._recheck_pause(self._recheck_fail_streak)
            self._recheck_next_at = time.monotonic() + pause
            logger.warning("Gap re-reads pause for %.0f min after %d "
                           "failed call(s) in a row", pause / 60,
                           self._recheck_fail_streak)
            break

    def _recheck_pause(self, streak):
        """Back-off after ``streak`` failed calls in a row, in seconds:
        INTERVAL x (2^streak - 1), at most RECHECK_BACKOFF_MAX."""
        interval = self.args.interval or self.config['daemon']['interval']
        return min(interval * (2 ** min(streak, 16) - 1), RECHECK_BACKOFF_MAX)

    @staticmethod
    def _new_fail(k, carry=False):
        """A failure record for cell k, held in hole['_fail']:

        n        failed calls in a row, as far as skipping goes
        calls    failed calls made for the cell in this pass, for the logs
                 (a regroup sets n back, not this)
        at       the window (ws, we) of the call that failed last; None for
                 a carry, which has not failed yet
        refused  the call that failed last got its answer, but a backend
                 did not take the records (see _recheck_pick)
        tried    when the cell was last read alone (monotonic), or None:
                 a carry counts as never read alone (see _alone_order)
        turn     its read alone comes in turn, with the cells read together:
                 only a window that failed twice, until a read alone of that
                 window failed (see _read_alone_decides)
        regroup  it may still go back to one failure (_read_alone_decides),
                 once per cell and pass

        A carry starts one failed call short of being skipped
        (RECHECK_CELL_MAX_FAILS - 1), with no call made yet, and is never in
        turn or regrouped.
        """
        return {'k': k, 'n': RECHECK_CELL_MAX_FAILS - 1 if carry else 0,
                'calls': 0, 'at': None, 'refused': False, 'tried': None,
                'turn': not carry, 'regroup': not carry}

    @staticmethod
    def _fail_record(e):
        """The failure record of the cell a candidate is about to read
        (see _new_fail), or None if that cell has not failed."""
        fail = e['hole'].get('_fail')
        return fail if fail and fail['k'] == e['k'] else None

    @classmethod
    def _cell_fails(cls, e):
        """Failed calls in a row for the cell a candidate is about to read."""
        fail = cls._fail_record(e)
        return fail['n'] if fail else 0

    def _recheck_pick(self, due, early, now):
        """The next call: (alone, c0, limit, batch, kept), or None.

        In turn, most overdue first (due time, then the hole's start): the
        cells read together, and the first read alone of a window whose
        calls failed twice in a row - it decides for the window's other
        cells (_read_alone_decides).  A cell that failed once comes before
        the others: it is the retry of the call that failed, so its batch
        stays together, also after a regroup.  The other cells read alone
        come after all of them: they are likely to fail again, and must
        hold up no cell read together.  Nothing is read alone while the
        alone back-off runs.

        Those after the others are of two kinds (_kept_kind).  A cell of
        the kept kind is its cell's last read: the tries of its grace day go
        by while it waits, and after the grace end one failed call gives it
        up.  The others get their calls later.  That costs nothing while
        the cloud and the bucket still hold their history, except for a
        cell whose read is its cell's last but whose last failed call a
        backend refused: it waits with the others, the tries of its grace
        day go by too, and after the grace end one failed call gives it up.
        With a bucket retention, history can age out meanwhile.  When both
        kinds are due they take every other call, and a kind that is due
        alone takes every call; ``kept`` tells the caller, which keeps the
        alternation, which kind the call is for (None: neither).  Within
        each kind, _alone_order.  For the others that means one gap at a
        time: a carry counts as never read alone, so a gap whose cells are
        being skipped one by one stays ahead of every cell already read
        alone.  Several gaps that keep failing - e.g. crawls through the
        cells older than a bucket's retention - therefore crawl one after
        another, the most overdue first, each one cell per step of the
        alone back-off (up to an hour), or per two steps while a cell of
        the kept kind is due.  Taking turns, each would crawl at a fraction
        of that speed, and all of them could fall behind the retention
        boundary, which moves an hour per hour.
        """
        alone_ok = (self._recheck_alone_next_at is None
                    or time.monotonic() >= self._recheck_alone_next_at)

        def in_turn(e):
            fail = self._fail_record(e)
            if fail is None or fail['n'] < 2:
                return True
            return alone_ok and fail['n'] == 2 and fail['turn']

        first = [e for e in due if in_turn(e)]
        kept = None
        if first:
            anchor = min(first, key=lambda e: (
                self._cell_fails(e) != 1, e['due_at'], e['hole']['start']))
            if self._cell_fails(anchor) < 2:
                c0, limit, batch = self._recheck_batch(anchor, due, early)
                return False, c0, limit, batch, None
        elif alone_ok and due:
            # Every cell due here failed twice at least and is not in turn.
            kinds = [(self._kept_kind(e, now), e) for e in due]
            kept_cells = [e for of_kept, e in kinds if of_kept]
            others = [e for of_kept, e in kinds if not of_kept]
            kept = bool(kept_cells) and (self._recheck_kept_next
                                         or not others)
            anchor = min(kept_cells if kept else others,
                         key=lambda e: self._alone_order(e, now))
        else:
            return None
        c = anchor['c']
        return (True, c, c + anchor['hole']['cell'] * 60 + RECHECK_CALL_PAD,
                [anchor], kept)

    def _kept_kind(self, e, now):
        """True if a cell read alone after the others is of the kept kind
        (see _recheck_pick): its read is its cell's last, and a backend did
        not refuse its last failed call.

        The cell's last read: a kept cell (RECHECK_CELL_MAX_FAILS failed
        calls on a last pass), or a twice-failed window no longer in turn
        (its window's read alone or its regroup failed, see
        _read_alone_decides) on the hole's last pass - 'at' is set: not a
        carry, which has not failed yet.  A carry on the last pass follows
        a cell given up after the grace end, so its single call comes after
        it wherever it sorts.

        A cell whose records a backend refused - most often points older
        than a bucket's retention, a 422 - cannot be written while it does:
        with a bucket of 7 days or less, every 7d read that finds samples
        the earlier reads did not is such a cell.  Its tries would be
        futile, and would halve the speed of every crawl through such cells
        while history ages out of the bucket.  So it waits with the others,
        and still gets its calls there; a later failed call that was not
        refused (the API failed) gives it its turn back.
        """
        fail = self._fail_record(e)
        if fail['refused']:
            return False
        return (fail['n'] >= RECHECK_CELL_MAX_FAILS
                or (fail['at'] is not None
                    and self._last_pass(e['hole'], now)))

    def _alone_order(self, e, now):
        """Sort key of the cells read alone after the others, within each
        kind (see _recheck_pick): never read alone first, then the least
        recently read alone, so that each gets its turn; ties by due time,
        then the hole's start.  A kept cell was always read alone (its
        third failed call was), so kept cells take turns, after a window
        demoted on its last pass, which never was.  A carry counts as never
        read alone, so the gap whose cells are being skipped goes on with
        its next cell, ahead of every cell read alone before.

        Past its grace end a kept cell among the others (its last failed
        call was refused, see _kept_kind) counts as never read alone too,
        if its hole has a next cell: its next failed call gives it up, and
        its hole goes on from there, like a carry.  It stands where its
        carry would stand had it been given up at the grace end without a
        call.  Sorted with the cells read alone, it could wait behind the
        crawls of less overdue gaps while its own gap's history aged out of
        the bucket.  A hole's last cell has no carry to stand for: its call
        only gives it up, and waits like any cell read alone.  A kept cell
        of the kept kind keeps its turn: put ahead of the kept cells still
        in their grace day, its single call would come at once, perhaps
        while the calls still fail, and give it up, and the others would
        lose turns of their grace day."""
        fail = self._fail_record(e)
        tried = fail['tried'] if fail else None
        if (tried is not None and fail['n'] >= RECHECK_CELL_MAX_FAILS
                and fail['refused']
                and now > self._grace_end(e['hole'])
                and e['k'] < _hole_ncells(e['hole']) - 1):
            tried = None
        return (tried is not None, tried or 0.0, e['due_at'],
                e['hole']['start'])

    def _recheck_batch(self, anchor, due, early):
        """(c0, limit, batch): the call window [c0 - pad, limit] and its holes.

        The window begins at the earliest next cell that starts at most
        RECHECK_CALL_PAD before the anchor's, so holes whose cells start a
        little earlier - sensors of one outage come back minutes apart -
        share the call.  It is as long as the anchor's cell plus the pads,
        and the anchor still fits.  Only cells read together join: one read
        alone could fail the others' call.  After one failed call - most
        are transient: a 5xx, a 429, a reset - the batch stays together:
        reading each hole alone would cost one call per sensor for the rest
        of the pass.
        """
        length = anchor['hole']['cell'] * 60
        c = anchor['c']
        pool = [e for e in due + early if self._cell_fails(e) < 2]
        c0 = min(e['c'] for e in pool
                 if c - RECHECK_CALL_PAD <= e['c'] <= c
                 and e['d'] - e['c'] <= length)
        limit = c0 + length + RECHECK_CALL_PAD
        return c0, limit, [e for e in pool
                           if e['c'] >= c0 and e['d'] <= limit]

    def _recheck_candidates(self, now, active, cap, sensors):
        """The holes a call could serve now: (due, early), with next cells.

        A hole is due once its next offset has passed, or while a pass is in
        progress.  It is early - allowed into a call that a due hole
        triggered - within _early_allowance() before its offset.  A due
        hole whose remaining cells are all older than MAX_BACKFILL finishes
        its pass here, without a call.
        """
        offs = self._recheck_offsets
        due, early = [], []
        for hole in list(self._recheck_holes):
            try:
                if hole['sid'] not in active or hole['done'] >= len(offs):
                    continue
                due_at = hole['closed_at'] + offs[hole['done']]
                is_due = hole['pos'] > 0 or now >= due_at
                k = self._next_cell(hole, cap)
                if k is None:
                    if is_due:
                        self._finish_pass(hole, now, sensors, skipped=True)
                    continue
                if not is_due and (hole['pos'] > 0 or now < due_at
                                   - self._early_allowance(hole['done'])):
                    continue
                c, d = _hole_cell(hole, k)
                entry = {'hole': hole, 'k': k, 'c': c, 'd': d,
                         'due_at': due_at}
                (due if is_due else early).append(entry)
            except Exception as exc:
                self._hole_error(hole, sensors, now, exc)
        return due, early

    def _window_text(self, ws, we):
        """'<start> -> <stop>' of a re-read window, for logs (never raises)."""
        try:
            return f'{self._window_str(ws)} -> {self._window_str(we)}'
        except (OverflowError, OSError, ValueError, TypeError):
            return f'{ws!r} -> {we!r}'

    def _writers_healthy(self):
        """True if every backend is connected and took its last write."""
        return all(w.connected and w._consecutive_failures == 0
                   for w in self.writers)

    def _recheck_writers_ready(self, waiting):
        """True if re-reads may write now; logs when they begin to wait.

        Re-read samples are older than the newest point a backend holds, so
        a backend that misses them never catches up on them.  Re-reads
        therefore wait - the gaps keep in state.json - while any backend is
        disconnected or failed its last write, the same test the deep scan
        applies.  It also keeps a failing backend's timeouts out of the
        re-reads' time budget.  Dry runs write nothing and never wait.
        Asked before each call, so never while no call is due, and again
        after a call whose write a backend refused; ``waiting`` is how many
        gaps are due.  Reads of what the polls may have missed are no gaps:
        while only they wait, both lines are DEBUG (with polls 40-59 min
        apart they are due most cycles).  The lines say what each backend
        did: it cannot be told whether a backend that failed its last write
        is down or refused only that call's records (a 422 for points
        beyond a bucket's retention), and the reader should not be told it
        is down when it took every other write.
        """
        if self.args.dryrun:
            return True
        failing = ['%s %s' % (type(w).__name__, 'is not connected'
                              if not w.connected else 'failed its last write')
                   for w in self.writers
                   if not w.connected or w._consecutive_failures > 0]
        if failing:
            if waiting and self._recheck_writers_waiting != 'warned':
                self._recheck_writers_waiting = 'warned'
                logger.warning("%d gap re-read(s) wait: %s; the gaps are kept "
                               "and re-read after a write works", waiting,
                               ', '.join(failing))
            elif not self._recheck_writers_waiting:
                self._recheck_writers_waiting = 'quiet'
                logger.debug("Re-reads wait: %s", ', '.join(failing))
            return False
        if self._recheck_writers_waiting:
            (logger.info if self._recheck_writers_waiting == 'warned'
             else logger.debug)("Every backend takes writes again; gap "
                                "re-reads resume")
            self._recheck_writers_waiting = False
        return True

    def _recheck_call(self, batch, ws, we, now, sensors, measurement_name,
                      cap):
        """One re-read call for a batch of holes: 'ok', 'truncated', 'failed'
        or 'unwritten'.

        A hole contributes records - its whole cell, margins included - only
        when its interior now holds more settled samples than any read
        found before; a probe on its single read always does.  Nothing
        advances unless the answer was complete and every record it
        produced reached every backend.  'failed': the call itself failed.
        'unwritten': the answer came in full, but a backend did not take
        its records (or the daemon is stopping).  The caller counts both
        alike; only _recheck_pick tells them apart.

        A probe is no gap, and with polls 40-59 min apart there is one per
        sensor nearly every poll: the probes of a call get one summary line
        instead of the gap lines, at INFO only if they found samples.
        """
        sids = sorted({e['hole']['sid'] for e in batch})
        gaps = [e for e in batch if not e['hole'].get('probe')]
        try:
            ws_str, we_str = self._window_str(ws), self._window_str(we)
            if gaps:
                logger.info("Re-reading %d gap(s) of %d sensor(s): %s -> %s",
                            len(gaps), len({e['hole']['sid'] for e in gaps}),
                            ws_str, we_str)
            else:
                logger.debug("Reading again what the polls may have missed "
                             "for %d sensor(s): %s -> %s", len(sids), ws_str,
                             we_str)
            self._touch()
            resp = self.api.get_samples(ws_str, we_str, measures=MEASURES,
                                        limit=(self.args.qlimit or 10000),
                                        sensors=sids)
            truncated = bool(resp.get('truncated'))
            data = resp.get('sensors') or {}
        except Exception as e:
            logger.warning("Gap re-read %s failed: %s",
                           self._window_text(ws, we), e)
            return 'failed'
        if truncated:
            return 'truncated'
        settled = now - RECHECK_SETTLED_AGE
        times = {}     # sid -> [(observed epoch, index in the answer)]
        taken = {}     # sid -> indices already contributed by this call
        records = []
        results = []
        for e in batch:
            hole = e['hole']
            before = len(records)
            try:
                sid = hole['sid']
                if sid not in times:
                    times[sid] = [(_observed_epoch(item.get('observed')), i)
                                  for i, item in enumerate(data.get(sid) or [])]
                last = e['k'] == _hole_ncells(hole) - 1
                cell = [(t, i) for t, i in times[sid]
                        if t is not None and e['c'] <= t
                        and (t < e['d'] or (last and t == e['d']))]
                interior = sorted({t for t, _ in cell
                                   if hole['start'] < t < hole['end']
                                   and t <= settled})
                n = len(interior)
                # A probe's margins can hold an own sample the window left
                # out even when its interior is empty: its one read writes.
                if n > hole['counts'][e['k']] or (hole.get('probe')
                                                  and hole['done'] == 0):
                    seen = taken.setdefault(sid, set())
                    new = sorted(i for _, i in cell if i not in seen)
                    if new:
                        records.extend(process_samples(
                            {'sensors': {sid: [data[sid][i] for i in new]}},
                            sensors, measurement_name,
                            self.config['my_altitude'], self.args.noconvert))
                        seen.update(new)
                hole.pop('_errors', None)
                results.append((e, n, interior, len(records) - before))
            except Exception as exc:
                self._hole_error(hole, sensors, now, exc)
        for i in range(0, len(records), RECHECK_WRITE_SLICE):
            part = records[i:i + RECHECK_WRITE_SLICE]
            if self.args.dryrun:
                self._log_dryrun(part)
            elif (not self._safe_write(part) or not self.running
                  or not self._writers_healthy()):
                # Also when only some backends took it: the others would
                # never get these samples otherwise.
                logger.warning("Writing gap re-reads failed on at least one "
                               "backend; they are read again later")
                return 'unwritten'
        probes = [(e, n, wrote) for e, n, _, wrote in results
                  if e['hole'].get('probe')]
        if probes:
            # Before the holes advance: their counts are still the old ones.
            new = sum(max(0, n - e['hole']['counts'][e['k']])
                      for e, n, _ in probes)
            (logger.info if new else logger.debug)(
                "Read again what the polls may have missed for %d sensor(s) "
                "(%s): %d new samples, %s",
                len({e['hole']['sid'] for e, _, _ in probes}),
                self._window_text(ws, we), new,
                'not written (dry run)' if self.args.dryrun
                else '%d written' % sum(wrote for _, _, wrote in probes))
        for e, n, interior, _ in results:
            try:
                self._advance_hole(e, n, interior, now, sensors, cap)
            except Exception as exc:
                self._hole_error(e['hole'], sensors, now, exc)
        return 'ok'

    def _recheck_truncated(self, batch, ws, we):
        """A re-read answer was truncated: read its holes in smaller cells.

        Halves the cells of the holes in the batch and returns True.  Their
        pass starts over with counts from zero, so the next reads write
        everything again and nothing is lost.  If every hole already has
        cells of RECHECK_CELL_MIN, a sensor has more samples in such a
        window than -q/--qlimit allows: returns False, and the caller counts
        the call as failed (the cell is tried again after the back-off - the
        live copies of the newest half hour may have gone by then - and
        skipped like any other failing cell).  The -q advice is logged once
        per pass of a hole.
        """
        qlimit = self.args.qlimit or 10000
        shrunk = [e for e in batch if e['hole']['cell'] > RECHECK_CELL_MIN]
        for e in shrunk:
            hole = e['hole']
            hole['cell'] = max(RECHECK_CELL_MIN, hole['cell'] // 2)
            hole['counts'] = [0] * _hole_ncells(hole)
            hole['pos'] = 0
            for key in _HOLE_PASS_KEYS:
                hole.pop(key, None)
        if shrunk:
            logger.warning("Gap re-read %s was truncated: a sensor has more "
                           "than %d samples (-q/--qlimit) in it; reading %d "
                           "gap(s) in %d-min cells instead",
                           self._window_text(ws, we), qlimit, len(shrunk),
                           min(e['hole']['cell'] for e in shrunk))
            return True
        new = [e for e in batch if not e['hole'].get('_qwarned')]
        (logger.warning if new else logger.debug)(
            "Gap re-read %s is truncated even in %d-min cells: a sensor has "
            "more than %d samples (-q/--qlimit) in it; a larger -q/--qlimit "
            "(0 means 10000) avoids it", self._window_text(ws, we),
            RECHECK_CELL_MIN, qlimit)
        for e in batch:
            e['hole']['_qwarned'] = True
        return False

    def _recheck_failed(self, batch, ws, we, now, sensors, cap, alone,
                        refused):
        """A re-read call failed: count it against each cell it was for.

        Records the call's window (see _read_alone_decides), whether a
        backend refused its records (``refused``, see _kept_kind) and, for
        a call read alone, when it was tried (see _alone_order).  A cell
        that reached RECHECK_CELL_MAX_FAILS failed calls in a row is then
        skipped, kept or given up by _cell_failed_out().
        """
        for e in batch:
            hole = e['hole']
            fail = self._fail_record(e)
            if fail is None:
                fail = hole['_fail'] = self._new_fail(e['k'])
            fail['n'] += 1
            fail['calls'] += 1
            fail['at'] = (ws, we)
            fail['refused'] = refused
            if alone:
                fail['tried'] = time.monotonic()
            if fail['n'] >= RECHECK_CELL_MAX_FAILS:
                self._cell_failed_out(hole, e['k'], fail, now, sensors, cap)

    def _cell_failed_out(self, hole, k, fail, now, sensors, cap):
        """Cell k failed RECHECK_CELL_MAX_FAILS calls in a row, or more.

        Not on the last pass, it is skipped for this pass, with a WARNING;
        its count stays, so the next pass reads it again.  A window the API
        or a backend always refuses then costs a few calls per pass and
        never stops the other re-reads.  The last pass has no next one: its
        cell is kept, and tried again alone in its turn.  Only a call that
        fails after the hole's grace end gives it up - never the clock
        alone: its last try may lie far back (the turns of other kept
        cells, a spell of failing polls), and it may work now.  A skipped
        or given-up cell's hole goes on with its next cell, as a carry
        (_skip_cell).  The WARNINGs give the failed calls actually made for
        the cell.
        """
        name = (sensors.get(hole['sid']) or {}).get('name', hole['sid'])
        text = self._window_text(*fail['at'])
        if not self._last_pass(hole, now):
            logger.warning("Re-reading %s for the gap in '%s' %s; skipped "
                           "for this pass, the next one reads it again",
                           text, name, _fmt_failed(fail['calls']))
        elif now <= self._grace_end(hole):
            if fail['n'] == RECHECK_CELL_MAX_FAILS:
                logger.warning(
                    "Re-reading %s for the gap in '%s' %s; this is its last "
                    "re-read, so it is tried again until it works or a call "
                    "after %s fails", text, name, _fmt_failed(fail['calls']),
                    _fmt_time(self._grace_end(hole)))
            return
        else:
            logger.warning("Re-reading %s for the gap in '%s' %s; its last "
                           "re-read is given up", text, name,
                           _fmt_failed(fail['calls']))
        self._skip_cell(hole, k, now, sensors, cap)

    def _grace_end(self, hole):
        """After this, a failed call gives up a kept cell of the hole's
        last pass (see _cell_failed_out)."""
        return (hole['closed_at'] + self._recheck_offsets[-1]
                + RECHECK_LAST_PASS_GRACE)

    def _skip_cell(self, hole, k, now, sensors, cap):
        """Leave cell k unread for this pass; its count stays as it was.

        Only for a cell whose calls kept failing: the hole's next cell is
        likely to fail as well, so it becomes a carry, one failed call short
        of being skipped - read alone, after the cells read together, which
        it can neither fail nor hold up.  A successful read clears that.
        The carry counts as never read alone (see _alone_order), so it
        comes before every cell already read alone: a gap that crawls
        through a run of failing cells, such as those older than a bucket's
        retention, goes on towards the cells that can be written, one cell
        per step of the alone back-off (one per two steps while a cell of
        the kept kind is due, see _recheck_pick), unless a more overdue
        gap's crawl is under way.
        """
        hole.setdefault('_since', now)
        hole.pop('_fail', None)
        hole.pop('_chain', None)
        hole['_partial'] = True
        hole['pos'] = k + 1
        nxt = self._next_cell(hole, cap)
        if nxt is not None:
            hole['pos'] = nxt
            hole['_fail'] = self._new_fail(nxt, carry=True)
        else:
            self._finish_pass(hole, now, sensors)

    def _read_alone_decides(self, read, failed, entries, worked):
        """A cell was read alone; decide for the others of its window.

        ``read`` is the candidate read alone, ``failed`` a copy of its
        failure record before the call, ``entries`` the candidates of the
        call.  Only a window that failed twice decides (not a carry or a
        kept cell); its other cells are those whose last failed call was
        the same window.
        - The read worked: most calls that fail, fail for all of their
          cells (a 5xx, a 429, a reset).  They go back to one failure, and
          _recheck_pick() puts them first, so the next call reads them
          together at that window: every hole of the failed batch moves on
          in step, and the hole read alone does not run ahead and split it.
          Once per cell and pass: if that call fails too, they are read
          alone.
        - It failed: the window is likely to fail for them too (a backend's
          retention, a window the API refuses).  They are read alone after
          the cells read together, so they hold up no cell read together:
          with the kept cells if the hole is on its last pass and no backend
          refused their records, else with the carries (see _kept_kind).
        """
        if failed.get('n') != 2 or failed.get('at') is None:
            return
        for e in entries:
            fail = self._fail_record(e)
            if (e['hole'] is read['hole'] or fail is None or fail['n'] != 2
                    or fail['at'] != failed['at']):
                continue
            if not worked:
                fail['turn'] = False
            elif failed['regroup'] and fail['regroup']:
                fail.update(n=1, turn=False, regroup=False)

    def _hole_error(self, hole, sensors, now, exc):
        """A hole could not be re-read (called from an except block)."""
        errors = hole.get('_errors', 0) + 1
        hole['_errors'] = errors
        name = (sensors.get(hole['sid']) or {}).get('name', hole['sid'])
        span = _fmt_span(hole['start'], hole['end'], now)
        if errors >= RECHECK_HOLE_MAX_ERRORS:
            logger.error("Dropping the gap in '%s' (%s) after %d failed "
                         "re-reads in a row", name, span, errors,
                         exc_info=True)
            self._drop_hole(hole)
        else:
            logger.warning("Could not re-read the gap in '%s' (%s): %s",
                           name, span, exc)
            logger.debug("Gap re-read error", exc_info=True)

    def _advance_hole(self, e, n, interior, now, sensors, cap):
        """Book a successful read of one cell; after the last, end the pass.

        Also follows the settled interior samples from cell to cell, so a
        pass that read every cell in order can show the hole is complete.
        """
        hole, k = e['hole'], e['k']
        hole.setdefault('_since', now)
        before = hole['counts'][k]
        hole['counts'][k] = max(before, n)
        hole['_found'] = hole.get('_found', 0) + n
        hole['_new'] = hole.get('_new', 0) + max(0, n - before)
        hole.pop('_fail', None)
        chain = ({'k': 0, 'first': None, 'last': None, 'inner': 0.0}
                 if k == 0 else hole.get('_chain'))
        if chain is not None and chain['k'] == k:
            for t in interior:
                if chain['last'] is None:
                    chain['first'] = t - hole['start']
                else:
                    chain['inner'] = max(chain['inner'], t - chain['last'])
                chain['last'] = t
            chain['k'] = k + 1
            hole['_chain'] = chain
        else:
            # A cell was left out (or the daemon restarted mid-pass).
            hole.pop('_chain', None)
        ncells = _hole_ncells(hole)
        logger.debug("Re-read cell %d/%d of the gap in sensor %s: %d "
                     "sample(s), %d before", k + 1, ncells, hole['sid'], n,
                     before)
        hole['pos'] = k + 1
        nxt = self._next_cell(hole, cap)
        if nxt is not None:
            hole['pos'] = nxt
            return
        chain = hole.pop('_chain', None)
        self._finish_pass(hole, now, sensors,
                          chain=chain if chain and chain['k'] == ncells
                          else None)

    def _last_pass(self, hole, now):
        """True if no re-read pass follows the hole's current one.

        That is the last offset's pass, and a pass that began - its first
        cell read or skipped ('_since') - at or after the last offset: the
        offsets after it went by while no re-read could run, and collapse
        into it.  A pass that has not begun counts as beginning now.  A pass
        that began before the last offset is not the last, even if it ends
        after it: the cells it read or skipped before re-reads stopped (the
        Pi off, a backend failing) get one more read.
        """
        offs = self._recheck_offsets
        return (hole['done'] >= len(offs) - 1
                or hole.get('_since', now) >= hole['closed_at'] + offs[-1])

    def _finish_pass(self, hole, now, sensors, chain=None, skipped=False):
        """A re-read pass over a hole is complete: schedule the next one.

        Offsets that went by while no re-read could run - the daemon was
        down, the polls or a backend failed - collapse into this pass, but
        the last one only if this was the last pass (see _last_pass): else
        the last offset's pass still follows, and reads every cell again.
        ``chain`` is set when the pass read every cell in order.  A hole of
        a sensor that records every minute is then dropped as soon as its
        interior is complete, and a probe as soon as it shows no gap; a long
        hole whose first pass found nothing skips ahead to the first offset
        of at least 3 days, if the schedule has one.
        """
        offs = self._recheck_offsets
        info = sensors.get(hole['sid']) or {}
        name = info.get('name', hole['sid'])
        span = _fmt_span(hole['start'], hole['end'], now)
        first = hole['done'] == 0
        partial = hole.get('_partial', False)
        done = max(hole['done'] + 1,
                   sum(1 for o in offs if hole['closed_at'] + o <= now))
        if done >= len(offs) and not self._last_pass(hole, now):
            done = len(offs) - 1
        hole['done'] = done
        hole['pos'] = 0
        found = hole.get('_found', 0)
        new = hole.get('_new', 0)
        for key in _HOLE_PASS_KEYS:
            hole.pop(key, None)
        probe = hole.get('probe')
        if skipped:
            result = 'older than MAX_BACKFILL, not read'
        elif probe:
            # Its one read wrote its whole cell, margins included, whatever
            # the interior held; the call's summary line says how much.
            result = '%d samples inside' % found
        elif new:
            result = '%d samples, %d new, %s' % (
                found, new,
                'not written (dry run)' if self.args.dryrun else 'written')
        else:
            result = '%d samples, nothing new' % found
        if partial:
            result += '; part of it could not be read'
        # A probe is no gap: its call has a summary line (_recheck_call).
        (logger.debug if probe else logger.info)(
            "Re-read %d/%d of the gap in '%s' (%s): %s",
            min(hole['done'], len(offs)), len(offs), name, span, result)
        if (info.get('type') in ONE_PER_MINUTE_TYPES and chain is not None
                and _interior_complete(hole, chain)):
            (logger.debug if probe else logger.info)(
                "Gap in '%s' %s is complete (%d samples); no more re-reads",
                name, span, found)
            self._drop_hole(hole)
            return
        if probe:
            if chain is not None and _largest_step(hole, chain) <= probe:
                logger.debug("No gap in '%s' %s; no more re-reads", name,
                             span)
                self._drop_hole(hole)
                return
            # A gap after all (or not all of it could be read): from now on
            # it is read as one.
            hole['probe'] = None
            if hole['done'] < len(offs):
                logger.info(
                    "Gap in '%s' %s: %s; re-reading it after %s", name, span,
                    'no data for %s' % _fmt_duration(_largest_step(
                        hole, chain)) if chain is not None
                    else 'not all of it could be read',
                    ', '.join(_fmt_offset(o) for o in offs[hole['done']:]))
        if (first and not partial and not sum(hole['counts'])
                and hole['end'] - hole['start'] > RECHECK_EMPTY_LONG):
            # Only where there is a later read to skip to: a schedule
            # without an offset of 3 days or more keeps all of its reads.
            later = [i for i, o in enumerate(offs)
                     if o >= RECHECK_EMPTY_SKIP_TO]
            if later and later[0] > hole['done']:
                hole['done'] = later[0]
                logger.info("Nothing in the gap in '%s' %s yet; next "
                            "re-read after %s", name, span,
                            _fmt_offset(offs[later[0]]))
        if hole['done'] >= len(offs):
            self._finish_hole(hole, sensors, now)

    def _finish_hole(self, hole, sensors, now):
        """All re-reads of a hole are done: drop it, with a summary."""
        name = (sensors.get(hole['sid']) or {}).get('name', hole['sid'])
        logger.info("Gap in '%s' %s: re-checks finished; the cloud holds %d "
                    "samples in it", name,
                    _fmt_span(hole['start'], hole['end'], now),
                    sum(hole['counts']))
        self._drop_hole(hole)

    def _backlog_min(self, setting, name):
        """A look-back-from-now window in minutes, at least MIN_POLL_BACKLOG_MIN.

        Shorter windows silently miss samples (see MIN_POLL_BACKLOG_MIN), so
        they are raised, with one warning per process.
        """
        minutes = parse_backlog(setting)
        if minutes >= MIN_POLL_BACKLOG_MIN:
            return minutes
        if not self._warned_backlog:
            self._warned_backlog = True
            logger.warning(
                "%s = %s is raised to %d min: the SensorPush API returns "
                "samples in ~15-minute chunks selected by their start time, "
                "so a shorter window silently misses most of them. Set it to "
                "1h (or more) to silence this warning.", name, setting,
                MIN_POLL_BACKLOG_MIN)
        return MIN_POLL_BACKLOG_MIN

    def _compute_oneshot_window(self, mytz, currenttime):
        """Compute time window for one-shot mode (backward compatible).

        An explicit -s start is used as given; note that samples in the
        first ~15 minutes after it can sit in a chunk that started earlier
        (see MIN_POLL_BACKLOG_MIN), so start a little early to be safe.
        """
        if self.args.starttime:
            starttime = datetime.datetime.strptime(
                self.args.starttime, '%Y-%m-%dT%X%z')
        else:
            backlog = self._backlog_min(self.args.backlog, '-b/--backlog')
            starttime = currenttime - datetime.timedelta(minutes=backlog)

        if self.args.stoptime:
            stoptime = datetime.datetime.strptime(
                self.args.stoptime, '%Y-%m-%dT%X%z')
        else:
            stoptime = currenttime

        return starttime, stoptime

    def _fetch_and_write_window(self, window, iteration, iterations,
                                sensors, measurement_name, sensor_filter=None):
        """Fetch samples for one time window and write to backend.

        Returns True if the window was fetched and written, False if it
        was abandoned after MAXRETRY attempts (daemon mode).  The caller
        must not treat a False as progress - see _record_failed_window.
        """
        if sensor_filter is None:
            sensor_filter = self.args.sensorlist
        retrycount = 0
        while True:
            try:
                self._touch()
                logger.info("Iteration %d/%d (%s -> %s)",
                            iteration, iterations, window[0], window[1])

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
                    sensors=sensor_filter
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
                    written = True
                else:
                    written = self._safe_write(records)
                    if written:
                        self._mark_success()

                # Gap detection.  Isolated: it can never fail, repeat or
                # change the window that was just handled.
                if self._recheck_on:
                    self._note_fetched_safely(samples, sensors, window,
                                              sensor_filter)

                # Delay between iterations
                if iterations > 1 and iteration < iterations:
                    logger.info("Sleeping for %d seconds", self.args.delay)
                    if self.args.daemon:
                        self._interruptible_sleep(self.args.delay)
                    else:
                        time.sleep(self.args.delay)
                return written

            except Exception as e:
                retrycount += 1
                logger.error("Error in iteration %d/%d (%s -> %s): %s",
                             iteration, iterations, window[0], window[1], e)

                if retrycount >= MAXRETRY:
                    if self.args.daemon:
                        # Report the window, not just the index - the index
                        # is meaningless once the cycle is over, and without
                        # the bounds the lost range cannot be found again.
                        logger.error(
                            "Max retries (%d) reached for window %s -> %s, "
                            "abandoning this attempt", MAXRETRY,
                            window[0], window[1])
                        return False
                    else:
                        logger.error(
                            "Max retries (%d) reached, stopping!",
                            MAXRETRY)
                        raise
                logger.info("Retrying in %ds (attempt %d/%d)",
                            RETRYWAIT, retrycount, MAXRETRY)
                self._interruptible_sleep(RETRYWAIT)

    def _safe_write(self, records):
        """Write records to all backends with retry and reconnection.

        Returns True if at least one backend accepted the batch.  In
        daemon mode a total failure is reported rather than raised, so
        the caller can decline to advance the high-water mark.
        """
        all_failed = True
        for writer in self.writers:
            if not self.running:
                return not all_failed

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

            backend_records = self._records_for(writer, records)
            for attempt, delay in enumerate([5, 10], 1):
                if not self.running:
                    return not all_failed
                try:
                    logger.debug("Writing %d records to %s (attempt %d)...",
                                 len(records), type(writer).__name__, attempt)
                    t0 = time.time()
                    writer.write(backend_records)
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
                        self._interruptible_sleep(delay)
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
        return not all_failed

    def _records_for(self, writer, records):
        """The records as this backend stores them.

        The very same objects unless the backend has SENSOR_UID on; then
        each point also carries the full SensorPush id as tag sensor_uid.
        sensor_id itself never changes, so the gap check (which filters on
        sensor_id) sees old and new series alike.
        """
        if not getattr(writer, 'sensor_uid', False):
            return records
        out = []
        for r in records:
            uid = self._sensor_uids.get((str(r['tags'].get('sensor_id')),
                                         str(r['tags'].get('sensor_name'))))
            if uid is None:
                out.append(r)
            else:
                out.append(dict(r, tags=dict(r['tags'], sensor_uid=uid)))
        return out

    def _log_dryrun(self, records):
        """Log records in dryrun mode, as each backend would store them."""
        if not any(getattr(w, 'sensor_uid', False) for w in self.writers):
            self._log_records(records, '')
            return
        for writer in self.writers:
            self._log_records(self._records_for(writer, records),
                              f' to {type(writer).__name__}')

    def _log_records(self, records, target):
        logger.info("--- Data that would have been written%s ---", target)
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


def display_sensors(sensors, sensor_uid=False):
    """Print every sensor, including the tag values its data is stored under."""
    for sid in sensors.keys():
        sensorname = sensors[sid]["name"]
        print(f'---------------{sensorname}---------------')
        print(f'stored as sensor_id: {sensor_id_tag(sid)}')
        if sensor_uid:
            print(f'stored as sensor_uid: {sid}')
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
    for tag, ids in find_sensor_id_collisions(sensors).items():
        print(f'WARNING: sensors {", ".join(ids)} are all stored as '
              f'sensor_id={tag}; sensor_id alone cannot tell them apart')


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    args = parse_args()

    # Setup logging
    setup_logging(level=args.loglevel, log_file=args.logfile)

    # The stored sensor_id is str(float(id)), which every CPython since 3.2
    # renders the same way ('short' repr).  A build without it would store
    # different values and start new series.
    if sys.float_repr_style != 'short':
        logger.warning(
            "This Python uses %r float repr; stored sensor_id values may "
            "differ from those written by a standard CPython build",
            sys.float_repr_style)

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

    # Force IPv4 once for the whole process, before any connection is made.
    if config['force_ipv4']:
        if enable_force_ipv4():
            logger.info("Forcing IPv4 for outbound connections")

    # Determine backend(s)
    if args.backend:
        backend_names = args.backend  # list from nargs='+'
    else:
        backend_names = [b.strip() for b in config['backend'].split(',')]

    # Only silence certificate warnings if a backend actually opts out of
    # verification, so a surprise on a verified connection still shows up.
    if any(config.get(name) and not config[name].get('verify_ssl', True)
           for name in ('influxdb2', 'influxdb3', 'victoriametrics')):
        suppress_insecure_warnings()

    # Handle list-only commands
    if args.listsensors or args.listgateways:
        api = SensorPushAPI(config['login'], config['password'],
                            verify_ssl=VERIFY_SSL)
        api.authenticate()
        if args.listgateways:
            display_gateways(api.get_gateways())
        if args.listsensors:
            display_sensors(api.get_sensors(), sensor_uid=any(
                (config.get(name) or {}).get('sensor_uid')
                for name in backend_names))
        return 0

    # Create API client and writers
    api = SensorPushAPI(config['login'], config['password'],
                        verify_ssl=VERIFY_SSL)
    logger.info("Active backend(s): %s", ', '.join(backend_names))
    writers = create_writers(backend_names, config)

    # Run
    daemon = SensorPushDaemon(api, writers, config, args)
    # Route the API client's retry sleeps through the daemon so they stay
    # interruptible and keep the watchdog heartbeat alive.
    api.sleep = daemon._interruptible_sleep

    if args.daemon:
        return daemon.run()

    daemon.run_once()
    return 0


if __name__ == '__main__':
    sys.exit(main())
