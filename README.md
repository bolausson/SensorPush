# SensorPush
[SensorPush](http://www.sensorpush.com/) recently introduced a [API](http://www.sensorpush.com/api/docs) to query the temperature and humidity samples recorded by their smart sensors via G1 WIFI GATEWAY - As long as the API is in beta stage, you have to contact support to get access!

This Python 3 tool can query the API and save the temperature and humidity time series to InfluxDB so it can easily be plotted with Grafana.

If you don't have an G1 WIFI Gateway and still want to plot your temperature, you can us another little tool I wrote to feed the CSV file which you can export via the Android App to InfluxDB.

![Grafana](https://github.com/bolausson/SensorPush/blob/master/SensorPush-Grafan-InfluxDB.jpg?raw=true)


## API query
```
# sensorpush.py --help
usage: sensorpush.py [-h] [-s STARTTIME] [-p STOPTIME] [-b BACKLOG]
                     [-t TIMESTEP] [-q QLIMIT] [-d DELAY] [-l] [-g]
                     [-i SENSORLIST [SENSORLIST ...]] [-x]

Queries SensorPus API and stores the temp and humidity readings in InfluxDB

optional arguments:
  -h, --help            show this help message and exit
  -s STARTTIME, --start STARTTIME
                        start query at time (e.g. "2019-07-25T00:10:41+0200")
  -p STOPTIME, --stop STOPTIME
                        Stop query at time (e.g. "2019-07-26T00:10:41+0200")
  -b BACKLOG, --backlog BACKLOG
                        Minutes back in time to fetch data (default 1440
                        minutes [24 h])
  -t TIMESTEP, --timestep TIMESTEP
                        Time slice per query (in minutes) to fetch (default
                        720 minutes [12 h])
  -q QLIMIT, --querylimit QLIMIT
                        Number of samples to return per sensor (default no
                        limit)
  -d DELAY, --delay DELAY
                        Delay in seconds between queries
  -l, --listsensors     Show a list of sensors and exit
  -g, --listgateways    Show a list of gateways and exit
  -i SENSORLIST [SENSORLIST ...], --sensorlist SENSORLIST [SENSORLIST ...]
                        List of sensor IDs to query
  -x, --dryrun          Do not write anything to the database, just print what
                        would have been written
```                        

## CSV import
```
# sensorpush_csv-import.py --help
usage: sensorpush_csv-import.py [-h] [-f CSVFILE] [-s SENSORNAME]
                                [-i SENSORID] [-d] [-c CHUNKS]

Reads a CSV file exported from the SensorPush Android App and stores the temp
and humidity readings in InfluxDB

optional arguments:
  -h, --help            show this help message and exit
  -f CSVFILE, --csvfile CSVFILE
                        CSV file exported from the SensorPush Android App
  -s SENSORNAME, --sensorname SENSORNAME
                        Sensor name
  -i SENSORID, --sensorid SENSORID
                        Sensor id
  -d, --dryrun          Do not write anything to InfluxDB - just print what
                        would have been written
  -c CHUNKS, --chunks CHUNKS
                        Write data in chunks to InfluxDB to not overload e.g.
                        a RaspberryPi
```
