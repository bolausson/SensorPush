#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# -----------------------------------------------------------------------------
# sensorpush_csv-import.py, Copyright Bjoern Olausson
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
# persistantly store temeprature an humidity values in InfluxDB
#

import sys
import uuid
import os
import csv
import json
import time
import datetime
import argparse
import configparser
import math
from pprint import pprint
from influxdb import InfluxDBClient
from pathlib import Path
from itertools import zip_longest


# __version__ = '1.1.1'
# __version_info__ = tuple([int(num) for num in __version__.split('.')])

homedir = str(Path.home())


CONFIGFILE = f'{homedir}/.sensorpush.conf'

config = configparser.ConfigParser()

if not Path(CONFIGFILE).is_file():
    config['INFLUXDBCONF'] = {
        'IFDB_IP': 'InfluxDP IP',
        'IFDB_PORT': 'InfluxDP port',
        'IFDB_USER': 'InfluxDP user',
        'IFDB_PW': 'InfluxDB password',
        'IFDB_DB': 'InfluxDP database'
    }
    with open(CONFIGFILE, 'w') as f:
        config.write(f)
else:
    config.read(CONFIGFILE)

IFDB_IP = config['INFLUXDBCONF']['IFDB_IP']
IFDB_PORT = int(config['INFLUXDBCONF']['IFDB_PORT'])
IFDB_USER = config['INFLUXDBCONF']['IFDB_USER']
IFDB_PW = config['INFLUXDBCONF']['IFDB_PW']
IFDB_DB = config['INFLUXDBCONF']['IFDB_DB']

parser = argparse.ArgumentParser(
    description='Reads a CSV file exported from the SensorPush Android App and\
    stores the temp and humidity readings in InfluxDB')
parser.add_argument(
    '-f',
    '--csvfile',
    dest='csvfile',
    default='',
    type=str,
    required=True,
    help='CSV file exported from the SensorPush Android App')
parser.add_argument(
    '-s',
    '--sensorname',
    dest='sensorname',
    default='',
    type=str,
    required=True,
    help='Sensor name')
parser.add_argument(
    '-i', '--sensorid',
    dest='sensorid',
    default='',
    type=str,
    required=True,
    help='Sensor id')
parser.add_argument(
    '-d', '--dryrun', dest='dryrun', action="store_true",
    help='Do not write anything to InfluxDB -\
    just print what would have been written')
parser.add_argument(
    '-c',
    '--chunks',
    dest='chunks',
    default='10000',
    type=int,
    help='Write data in chunks to InfluxDB to not overload e.g. a RaspberryPi')
args = parser.parse_args()

csvfile = args.csvfile
sensorname = args.sensorname
sensorid = args.sensorid
dryrun = args.dryrun
chunks = args.chunks

if not sensorname:
    print("You must specify a sensor name!")
    sys.exit(0)
    #sensorname = os.path.splitext(csvfile)[0]

if not sensorid:
    print("You must specify the sensor ID e.g.: 123456.123456789012345!")
    sys.exit(0)
    # sensorid = str(uuid.uuid4())
    #sensorid = sensorname


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def local_time_offset(t=None):
    """Return offset of local zone from GMT, either at present or at time t."""
    # python2.3 localtime() can't take None
    if t is None:
        t = time.time()

    if time.localtime(t).tm_isdst and time.daylight:
        return -time.altzone / 3600
    else:
        return -time.timezone / 3600


ifdbc = InfluxDBClient(host=IFDB_IP,
                       port=IFDB_PORT,
                       username=IFDB_USER,
                       password=IFDB_PW,
                       database=IFDB_DB)


# Try to get the proper UTC time offseet --------------------------------------
mytz = datetime.timezone(datetime.timedelta(hours=local_time_offset()))
currenttime = datetime.datetime.now(tz=mytz)

measurement = []

with open(csvfile) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print(f'Column headers are {", ".join(row)}')
            timeshead, temphead, humihead = row
            line_count += 1
        else:
            dtime = datetime.datetime.strptime(
                row[0],
                '%Y-%m-%d %H:%M').isoformat()
            celsius = float(row[1].replace(',', '.'))
            hum = float(row[2].replace(',', '.'))
            measurement.extend([
                {
                               'measurement': 'SensorPush',
                               'tags': {
                                   'sensor_id': sensorid,
                                   'sensor_name': sensorname,
                               },
                               'fields': {
                                   'temperature': celsius,
                                   'humidity': hum,
                               },
                               # 'observed': '2019-07-26T15:09:18.000Z
                               'time': dtime
                               }
            ])
            line_count += 1

# pprint(measurement)
numsamples = len(measurement)
pprint(f'Samples: {numsamples}')

iterations = math.ceil(numsamples / chunks)

g = grouper(measurement, chunks)

i = 1
for item in g:
    pprint(f'Writing batch {i}/{iterations} to influxdb')
    # Since grouper fills a group with None, we have to get rid of those
    # before writing it to InfluxDB
    b = [j for j in item if j]
    if len(b):
        if dryrun:
            pprint(b)
            pprint('--------------------')
            pprint('')
        else:
            ifdbc.write_points(b)
    else:
        pprint('Nothing to write for this chunk')

    i += 1
