#!/usr/bin/env python3

import random
import os
import threading
import time
# requires that influxdb python driver be installed
# try using  pip install influxdb
from influxdb import InfluxDBClient 

DBNAME = 'market_data'
# initialize database connection
client = InfluxDBClient(host="localhost", port=8086, database=DBNAME) 


def read_and_write(sym, file):
    print("processing file %s \n" % (file))
    with open(file) as f:
        for line in f:
            fields = line.split(",")
            ts_data = "trade_tick,sym=%s ask=%s,bid=%s" % (sym, fields[1], fields[2])   # using the line protocol
            client.write(ts_data, {'db' : DBNAME}, protocol='line')
            print("%s" % (ts_data))
            time.sleep(5)
    print("done\n")


def execute():
    dataset_dir = os.path.join(os.getcwd(), "dataset")
    files = [file for file in os.listdir(dataset_dir) if file[-3:] == 'csv']
    no_of_files = len(files)
    if no_of_files == 0:
        raise Exception("could not find any csv files in dataset folder")

    threads = list()
    for index in range(no_of_files):
        file_to_read = os.path.join(dataset_dir, files[index])
        thread = threading.Thread(target=read_and_write, args=(files[index][0:-4], file_to_read,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

# execute the generate function
execute()
