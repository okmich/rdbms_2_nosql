#!/usr/bin/env python3

import random
import time
from influxdb import InfluxDBClient

def get_next_reading():
	BASE_READING = 36.8
	return BASE_READING + random.random()


DBNAME = 'hellodb'
client = None

def generate() :
	client = InfluxDBClient(host="localhost", port=8086, database=DBNAME) 
	# In a continous loop, generate random temperature reading and send to influxdb server
	try :
		while True:
			ts_data = "temp_measure reading=%.4f"  % (get_next_reading()) # using the line protocol
			client.write(ts_data, {'db':DBNAME}, protocol='line') 
			print("%s" % (ts_data))
			time.sleep(5)
	except KeyboardInterrupt as e:
		client.close()
		raise e

# execute the generate function
generate()
