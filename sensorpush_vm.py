#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# sensorpush_vm.py, Copyright Bjoern Olausson
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
# -----------------------------------------------------------------------------
#
# This Python 3 program is intended to query the SensorPush API and
# persistantly store temperature and humidity values in VictoriaMetrics
#

import sys
import json
import time
import math
import requests
import datetime
import argparse
import configparser
from pathlib import Path
from pprint import pprint
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Ignore SSL errors and suppress InsecureRequestWarning
VERIFY_SSL=False

if not VERIFY_SSL:
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

homedir = str(Path.home())

CONFIGFILE = f'{homedir}/.sensorpush_vm.conf'

RETRYWAIT = 60
MAXRETRY = 3

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

config = configparser.ConfigParser()

if not Path(CONFIGFILE).is_file():
    config['SONSORPUSHAPI'] = {
        'LOGIN': 'SensorPush login',
        'PASSWD': 'SensorPush password'
    }
    config['VICTORIAMETRICSCONF'] = {
        'MEASUREMENT_NAME': 'SensorPush',
        'VM_URL': 'http://localhost:8428',
        'VM_VERIFY_SSL': 'False'
    }
    config['MISC'] = {
        'MY_ALTITUDE': 'Metres above sea level',
        'FORCE_IPv4': 'False'
    }
    with open(CONFIGFILE, 'w') as f:
        config.write(f)
    print(f'Created config file template at {CONFIGFILE}')
    print('Please edit the config file with your settings and run again.')
    sys.exit(0)
else:
    config.read(CONFIGFILE)

LOGIN = config['SONSORPUSHAPI']['LOGIN']
PASSWD = config['SONSORPUSHAPI']['PASSWD']
MEASUREMENT_NAME = config['VICTORIAMETRICSCONF']['MEASUREMENT_NAME']
VM_URL = config['VICTORIAMETRICSCONF']['VM_URL']
VM_VERIFY_SSL = config['VICTORIAMETRICSCONF'].getboolean('VM_VERIFY_SSL', False)

MY_ALTITUDE = config['MISC']['MY_ALTITUDE']
FORCE_IPv4 = config['MISC'].getboolean('FORCE_IPv4', False)

try:
    MY_ALTITUDE = float(MY_ALTITUDE)
except ValueError as e:
    print(f"MY_ALTITUDE is not set to a valid number: {MY_ALTITUDE}")
    sys.exit()

parser = argparse.ArgumentParser(
    description='Queries SensorPush API and stores the temp and humidity\
                readings in VictoriaMetrics')
parser.add_argument(
    '-s',
    '--start',
    dest='starttime',
    default='',
    type=str,
    help='start query at time (e.g. "2019-07-25T00:10:41+0200")')
parser.add_argument(
    '-p',
    '--stop',
    dest='stoptime',
    default='',
    type=str,
    help='Stop query at time (e.g. "2019-07-26T00:10:41+0200")')
parser.add_argument(
    '-b',
    '--backlog',
    dest='backlog',
    default='1d',
    type=str,
    help='Historical data to fetch (default 1 day) - time can be specified in the format <number>[m|h|d|w|M|Y]. E.g.: 10 Minutes = 10m, 1 day = 1d, 1 month = 1M')
parser.add_argument(
    '-t',
    '--timestep',
    dest='timestep',
    default='720',
    type=int,
    help='Time slice per query (in minutes) to fetch\
         (default 720 minutes [12 h])')
parser.add_argument(
    '-q',
    '--querylimit',
    dest='qlimit',
    default='0',
    type=int,
    help='Number of samples to return per sensor (default unset = API default limimt [10])')
parser.add_argument('-d',
                    '--delay',
                    dest='delay',
                    default='60',
                    type=int,
                    help='Delay in seconds between queries')
parser.add_argument(
    '-l',
    '--listsensors',
    dest='listsensors',
    action='store_true',
    help='Show a list of sensors and exit')
parser.add_argument(
    '-g',
    '--listgateways',
    dest='listgateways',
    action='store_true',
    help='Show a list of gateways and exit')
parser.add_argument(
    '-i',
    '--sensorlist',
    dest='sensorlist',
    nargs='+',
    type=str,
    help='List of sensor IDs to query')
parser.add_argument(
    '-n',
    '--noconvert',
    dest='noconvert',
    action='store_true',
    help='Do not convert °F to °C, inHG to mBar, kPa to mBar and feet to meters')
parser.add_argument(
    '-x',
    '--dryrun',
    dest='dryrun',
    action='store_true',
    help='Do not write anything to the database,\
        just print what would have been written')

args = parser.parse_args()

starttime = args.starttime
stoptime = args.stoptime
timesteps = args.timestep
qlimit = args.qlimit
delay = args.delay
listsensors = args.listsensors
listgateways = args.listgateways
sensorlist = args.sensorlist
noconvert = args.noconvert
dryrun = args.dryrun

backlogstring = args.backlog

# Convert backlog to minutes
minutes_per_unit = {"m": 1, "h": 60, "d": 60*24, "w": 60*24*7, "M": 60*24*30.417, "Y": 60*24*365}
backlog = int(int(backlogstring[:-1]) * minutes_per_unit[backlogstring[-1]])

def local_time_offset(t=None):
    """Return offset of local zone from GMT, either at present or at time t."""
    # python2.3 localtime() can't take None
    if t is None:
        t = time.time()

    if time.localtime(t).tm_isdst and time.daylight:
        return -time.altzone / 3600
    else:
        return -time.timezone / 3600


def F_to_C(F):
    if noconvert:
        return float(F)
    else:
        try:
            C = float(round((F - 32) * 5.0 / 9.0,2))
        except TypeError as e:
            C = 0.0
        return C

def ft_to_m(ft):
    if noconvert:
        return float(ft)
    else:
        m = float(round(ft * 0.3048,2))
        return m

def inHg_to_mBar(inHg):
    if noconvert:
        return float(inHg)
    else:
        mbar = float(round(inHg * 33.8639,2))
        return mbar

def kPa_to_mBar(kPa):
    if noconvert:
        return float(kPa)
    else:
        mBar = float(round(kPa * 10,2))
        return mBar

def escape_tag_value(value):
    """Escape special characters in InfluxDB line protocol tag values."""
    value = str(value)
    value = value.replace(',', '\\,')
    value = value.replace('=', '\\=')
    value = value.replace(' ', '\\ ')
    return value

def escape_field_key(key):
    """Escape special characters in InfluxDB line protocol field keys."""
    key = str(key)
    key = key.replace(',', '\\,')
    key = key.replace('=', '\\=')
    key = key.replace(' ', '\\ ')
    return key

def to_influx_line_protocol(measurement_name, tags, fields, timestamp):
    """
    Convert measurement data to InfluxDB line protocol format.

    Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
    """
    # Build tag set
    tag_set = ','.join([f'{escape_tag_value(k)}={escape_tag_value(v)}' for k, v in tags.items()])

    # Build field set
    field_parts = []
    for k, v in fields.items():
        key = escape_field_key(k)
        # Handle different field value types
        if isinstance(v, bool):
            field_parts.append(f'{key}={str(v).lower()}')
        elif isinstance(v, int):
            field_parts.append(f'{key}={v}i')
        elif isinstance(v, float):
            field_parts.append(f'{key}={v}')
        elif isinstance(v, str):
            # String values need to be quoted
            field_parts.append(f'{key}="{v}"')
        else:
            field_parts.append(f'{key}={v}')

    field_set = ','.join(field_parts)

    # Convert timestamp to nanoseconds (InfluxDB line protocol expects nanoseconds)
    # Parse ISO format timestamp
    if isinstance(timestamp, str):
        # Parse the timestamp string to datetime
        try:
            dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            # Try alternative parsing
            dt = datetime.datetime.strptime(timestamp.split('.')[0], '%Y-%m-%dT%H:%M:%S')
        timestamp_ns = int(dt.timestamp() * 1e9)
    else:
        timestamp_ns = int(timestamp * 1e9)

    # Construct line protocol
    line = f'{measurement_name},{tag_set} {field_set} {timestamp_ns}'
    return line

def write_to_victoriametrics(lines):
    """Write data to VictoriaMetrics using InfluxDB line protocol."""
    if not lines:
        return

    # Join all lines with newline
    data = '\n'.join(lines)

    # POST to VictoriaMetrics /write endpoint
    url = f'{VM_URL}/write'

    try:
        # Encode data as UTF-8 to properly handle special characters (e.g., umlauts)
        response = requests.post(
            url,
            data=data.encode('utf-8'),
            headers={'Content-Type': 'text/plain; charset=utf-8'},
            verify=VM_VERIFY_SSL
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Error writing to VictoriaMetrics: {e}', file=sys.stderr)
        raise


# Try to get the proper UTC time offset --------------------------------------
mytz = datetime.timezone(datetime.timedelta(hours=local_time_offset()))
currenttime = datetime.datetime.now(tz=mytz)
querytime = currenttime

if not starttime:
    starttime = currenttime - datetime.timedelta(minutes=int(backlog))
else:
    starttime = datetime.datetime.strptime(args.starttime, '%Y-%m-%dT%X%z')

if not stoptime:
    stoptime = currenttime
else:
    stoptime = datetime.datetime.strptime(args.stoptime, '%Y-%m-%dT%X%z')

starttimestr = 'Start: ' + datetime.date.strftime(starttime, '%Y-%m-%dT%X%z')
stoptimestr = 'Stop:  ' + datetime.date.strftime(stoptime, '%Y-%m-%dT%X%z')

timelist = []
while starttime <= stoptime:
    start = datetime.date.strftime(starttime, '%Y-%m-%dT%X%z')

    try:
        nextstop = newstartt + datetime.timedelta(minutes=int(timesteps))
    except BaseException:
        nextstop = starttime + datetime.timedelta(minutes=int(timesteps))

    stop = datetime.date.strftime(nextstop, '%Y-%m-%dT%X%z')

    currenttime = datetime.datetime.now(tz=mytz)
    starttime = nextstop - datetime.timedelta(minutes=30)
    newstartt = nextstop

    timelist.extend([[start, stop]])

    iterations = len(timelist)

# Setup requests ----------------------------------------------------
s = requests.Session()
s.mount(API_URL_BASE, HTTPAdapter(max_retries=10))

# get API oauth authorization string ------------------------------------------
HTTP_DATA = json.dumps({'email': LOGIN, 'password': PASSWD})

trycount = 0
while True:
    trycount += 1
    pprint(f'Fetching API oauth authorization string - try {trycount}/{MAXRETRY}')
    try:
        r = s.post(API_URL_OA_AUTH,
                   headers=HTTP_OA_HEAD,
                   data=HTTP_DATA,
                   verify=VERIFY_SSL)
    except requests.exceptions.ConnectionError as e:
        time.sleep(20)
    else:
        break

    if trycount >= MAXRETRY:
        print(f'Failed to fetch API oauth authorization string - giving up after {trycount} attempts!', file=sys.stderr)
        print(f'Failed to fetch API oauth authorization string - giving up after {trycount} attempts!')
        sys.exit()

if r.status_code == 200:
    auth = r.content.decode('utf-8')
else:
    pprint('Auth request failed')
    pprint(r)
    sys.exit()


# get API oauth access token --------------------------------------------------
pprint('Fetching API oauth access token')
HTTP_DATA = auth
r = s.post(API_URL_OA_ATOK,
           headers=HTTP_OA_HEAD,
           data=HTTP_DATA,
           verify=VERIFY_SSL)

if r.status_code == 200:
    atok = json.loads(r.content.decode('utf-8'))['accesstoken']
else:
    pprint('Access token request failed')
    pprint(r)
    sys.exit()

# Create header for further requests:
HTTP_HEAD = {'accept': 'application/json',
             'Authorization': atok}

# Get a list of gateways ------------------------------------------------------
pprint('Fetching the list of gateways')
HTTP_DATA = json.dumps({})
r = s.post(API_URL_GW,
           headers=HTTP_HEAD,
           data=HTTP_DATA,
           verify=VERIFY_SSL)

if r.status_code == 200:
    gateways = json.loads(r.content.decode('utf-8'))
else:
    pprint('Could not fetch the list of gateways')
    pprint(r)
    sys.exit()

if listgateways:
    for id in gateways.keys():
        gwname = gateways[id]["name"]
        print(f'---------------{gwname}---------------')
        print(f'Last alert               : {gateways[id]["last_alert"]}')
        print(f'Last seen                : {gateways[id]["last_seen"]}')
        print(f'Message                  : {gateways[id]["message"]}')
        print(f'Paired                   : {gateways[id]["paired"]}')
        print(f'Version                  : {gateways[id]["version"]}')
        print('------------------------------------------------------------')
        print('')

    sys.exit(0)


# Get a list of bulk reports --------------------------------------------------
pprint('Fetching the list of bulk reports')
HTTP_DATA = json.dumps({})
r = s.post(API_URL_RPL,
           headers=HTTP_HEAD,
           data=HTTP_DATA,
           verify=VERIFY_SSL)

if r.status_code == 200:
    reports = json.loads(r.content.decode('utf-8'))
else:
    pprint('Could not fetch the list of bulk reports')
    pprint(r)
    sys.exit()

if len(reports["files"]) > 0:
    print("Bulk reports to download:")
    for file in reports["files"]:
        pprint(file)

# Get a list of sensors -------------------------------------------------------
pprint('Fetching the list of sensors')
HTTP_DATA = json.dumps({})
r = s.post(API_URL_SE,
           headers=HTTP_HEAD,
           data=HTTP_DATA,
           verify=VERIFY_SSL)

if r.status_code == 200:
    sensors = json.loads(r.content.decode('utf-8'))
else:
    pprint('Could not fetch the list of sensors')
    pprint(r)
    sys.exit()

measurement_v_lines = []
measurement_v_name = f'{MEASUREMENT_NAME}_V'
print(measurement_v_name)

for id in sensors.keys():
    if listsensors:
        sensorname = sensors[id]["name"]
        print(f'---------------{sensorname}---------------')

        for key in sensors[id].keys():
            print(f'{key}: {sensors[id][key]}')

    try:
        BatVolt = float(sensors[id]["battery_voltage"])
    except KeyError as e:
        if listsensors:
            print(f'Failed to get battery_voltage for {sensors[id]["name"]}')
        BatVolt = 0

    try:
        RSSI = float(sensors[id]["rssi"])
    except KeyError as e:
        if listsensors:
            print(f'Failed to get rssi for {sensors[id]["name"]}')
        RSSI = 0

    if listsensors:
        print('------------------------------------------------------------')
        print('')

    # Convert to InfluxDB line protocol
    tags = {
        'sensor_id': str(sensors[id]["id"]),
        'sensor_name': str(sensors[id]["name"])
    }
    fields = {
        'voltage': float(BatVolt),
        'rssi': float(RSSI)
    }
    timestamp = datetime.date.strftime(querytime, '%Y-%m-%dT%X%z')

    line = to_influx_line_protocol(measurement_v_name, tags, fields, timestamp)
    measurement_v_lines.append(line)

if listsensors:
    sys.exit(0)
else:
    if dryrun:
        pprint('------------Data that would have been written---------')
        for line in measurement_v_lines:
            pprint(line)
        pprint('------------------------------------------------------')
    else:
        write_to_victoriametrics(measurement_v_lines)

# Get samples -----------------------------------------------------------------
pprint('-------------------------------------------------------------------')
pprint(starttimestr)
pprint(stoptimestr)
pprint('-------------------------------------------------------------------')

pprint('Iterations required: ' + str(iterations))
pprint('-------------------------------------------------------------------')

iteration = 1
retrycount = 0

measures = ["altitude","barometric_pressure","dewpoint","humidity","temperature","vpd","distance"]

for item in timelist:
    failed = True

    while failed:
        try:
            pprint(f'Iteration {iteration}/{iterations}')

            query = {'startTime': item[0], 'stopTime': item[1], 'measures': measures}

            if qlimit != 0:
                query['limit'] = qlimit

            if sensorlist:
                query['sensors'] = sensorlist

            HTTP_DATA = json.dumps(query)

            r = s.post(API_URL_SPL,
                       headers=HTTP_HEAD,
                       data=HTTP_DATA,
                       verify=VERIFY_SSL)

            if r.status_code == 200:
                samples = json.loads(r.content.decode('utf-8'))
            else:
                raise ValueError('Could not fetch samples')

            truncated = samples['truncated']
            numsamples = samples['total_samples']
            numsensosrs = samples['total_sensors']

            pprint('Request truncated: ' + str(truncated))

            if truncated:
                pprint('You might want to consider reducing the time slices')

            pprint('Number of samples fetched: ' + str(numsamples))
            pprint('Number of sensors queried: ' + str(numsensosrs))

            # Convert data to InfluxDB line protocol ----------------------------
            measurement_lines = []
            for key in samples['sensors'].keys():
                for item in samples['sensors'][key]:
                    observed = str(item['observed'])
                    sensor_id = str(key)
                    sensor_name = str(sensors[key]['name'])

                    tags = {
                        'sensor_id': sensor_id,
                        'sensor_name': sensor_name
                    }

                    fields = {}

                    try:
                        humidity = float(item['humidity'])
                    except KeyError as e:
                        pass
                    else:
                        fields['humidity'] = float(humidity)

                    try:
                        temperature = F_to_C(item['temperature'])
                    except KeyError as e:
                        pass
                    else:
                        fields['temperature'] = float(temperature)

                    try:
                        pressure = inHg_to_mBar(item['barometric_pressure'])
                    except KeyError as e:
                        # Absolute humidity (g/m³)
                        # https://carnotcycle.wordpress.com/2012/08/04/how-to-convert-relative-humidity-to-absolute-humidity/
                        abs_humidity = float(round((6.112 * math.e**((17.67 * temperature)/(temperature + 243.5)) * humidity * 2.1674) / (273.15 + temperature),2))
                        fields['abs_humidity'] = float(abs_humidity)
                    else:
                        fields['pressure'] = float(pressure)
                        # Absolute humidity (g/m³)
                        # https://www.loxwiki.eu/display/LOX/Absolute+Luftfeuchtigkeit+berechnen
                        abs_humidity = float(round(0.622 * humidity / 100 * (1.01325 * 10.0**(5.426651 - 2005.1 / (temperature + 273.15) + 0.00013869 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0) / (temperature + 273.15) * (10.0**(0.000000000011965 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0) * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) - 1.0) - 0.0044 * 10.0**((-0.0057148 * (374.11 - temperature)**1.25))) + (((temperature + 273.15) / 647.3) - 0.422) * (0.577 - ((temperature + 273.15) / 647.3)) * math.exp(0.000000000011965 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0) * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) * 0.00980665) / (pressure / 1000.0 - humidity / 100.0 * (1.01325 * 10.0**(5.426651 - 2005.1 / (temperature + 273.15) + 0.00013869 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0) / (temperature + 273.15) * (10.0**(0.000000000011965 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0) * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) - 1.0) - 0.0044 * 10.0**((-0.0057148 * (374.11 - temperature)**1.25))) + (((temperature + 273.15) / 647.3) - 0.422) * (0.577 - ((temperature + 273.15) / 647.3)) * math.exp(0.000000000011965 * ((temperature + 273.15) * (temperature + 273.15) - 293700.0) * ((temperature + 273.15) * (temperature + 273.15) - 293700.0)) * 0.00980665)) * pressure/1000.0 * 100000000.0 / ((temperature + 273.15) * 287.1),2))
                        fields['abs_humidity'] = float(abs_humidity)

                    try:
                        altitude = ft_to_m(item['altitude'])
                    except KeyError as e:
                        altitude = float(MY_ALTITUDE)
                    finally:
                        if altitude == 0:
                            altitude = float(MY_ALTITUDE)
                        fields['altitude'] = float(altitude)

                    try:
                        distance = ft_to_m(item['distance'])
                    except KeyError as e:
                        pass
                    else:
                        fields['distance'] = float(distance)

                    try:
                        dewpoint = F_to_C(item['dewpoint'])
                    except KeyError as e:
                        # Dewpoint in degree centigrate
                        # https://cals.arizona.edu/azmet/dewpoint.html
                        dewpoint = float(round((237.3 * ((math.log(humidity / 100) + ((17.27 * temperature) / (237.3 + temperature))) / 17.27)) / (1 - ((math.log(humidity / 100) + ((17.27 * temperature) / (237.3 + temperature))) / 17.27)),2))
                    finally:
                        fields['dewpoint'] = float(dewpoint)

                    try:
                        vpd = kPa_to_mBar(item['vpd'])
                    except KeyError as e:
                        # Vapor Pressure Deficit in mBar
                        # https://pulsegrow.com/blogs/learn/vpd
                        vpd = float(kPa_to_mBar(((610.78 * math.e**(temperature / (temperature + 238.3) * 17.2694)) / 1000) * (1 - humidity/100)))
                    finally:
                        fields['vpd'] = float(vpd)

                    # Convert to InfluxDB line protocol
                    line = to_influx_line_protocol(MEASUREMENT_NAME, tags, fields, observed)
                    measurement_lines.append(line)

            if dryrun:
                pprint('------------Data that would have been written---------')
                for line in measurement_lines[:5]:  # Show first 5 lines as sample
                    pprint(line)
                if len(measurement_lines) > 5:
                    pprint(f'... and {len(measurement_lines) - 5} more lines')
                pprint('------------------------------------------------------')
            else:
                write_to_victoriametrics(measurement_lines)

            iteration += 1

            if iterations > 1:
                pprint('------------------------------------------------------')
                pprint(f'sleeping for {delay} seconds')
                pprint('------------------------------------------------------')

                time.sleep(delay)

        except Exception as e:
            retrycount += 1
            pprint('')
            pprint('##################Somthing went wrong################')
            pprint('~~~~~~~~~~~~Exception~~~~~~~~~~~~~')
            pprint(e)
            pprint('~~~~~~~~~~~~Request status code~~~~~~~~~~~~~')
            pprint(r.status_code)
            pprint('~~~~~~~~~~~~~~~~~~~~~~~~~')
            pprint(r.headers)
            pprint('~~~~~~~~~~~~Request content~~~~~~~~~~~~~')
            pprint(r.content.decode('utf-8'))
            pprint('------------------------------------------------------')
            pprint(f'Retrying iteration {iteration}/{iterations}')
            pprint(f'Try {retrycount}/{MAXRETRY}')
            pprint(f'sleeping for {RETRYWAIT} seconds before next try')
            pprint('######################################################')
            pprint('')
            if retrycount >= MAXRETRY:
                pprint('Reached max retries ({MAXRETRY}) Stopping now!')
                sys.exit()
            time.sleep(RETRYWAIT)
            continue
        else:
            failed = False


