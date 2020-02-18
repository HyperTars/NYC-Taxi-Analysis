#!/usr/bin/env python
import os
import string
import csv
csvfile = open('./samples/trips_samp.csv', 'r')
csvreader = csv.DictReader(csvfile)
#csvreader = csv.DictReader(sys.stdin)
attr = []
for row in csvreader:
    attr = row.keys()
    break
tripFlag = False
if 'rate_code' in attr:
    tripFlag = True
print len(attr)
for row in csvreader:
    key = ''
    value = ''
    key = str(row['medallion']) + ',' + \
            str(row['hack_license']) + ',' + \
            str(row['vendor_id']) + ',' + \
            str(row['pickup_datetime'])
    
    if tripFlag:
        value = str(row['rate_code']) + ',' + \
            str(row['store_and_fwd_flag']) + ',' + \
            str(row['dropoff_datetime']) + ',' + \
            str(row['passenger_count']) + ',' + \
            str(row['trip_time_in_secs']) + ',' + \
            str(row['trip_distance']) + ',' + \
            str(row['pickup_longitude']) + ',' + \
            str(row['pickup_latitude']) + ',' + \
            str(row['dropoff_longitude']) + ',' + \
            str(row['dropoff_latitude'])
    else:
        value = str(row['payment_type']) + ',' + \
            str(row['fare_amount']) + ',' + \
            str(row['surcharge']) + ',' + \
            str(row['mta_tax']) + ',' + \
            str(row['tip_amount']) + ',' + \
            str(row['tolls_amount']) + ',' + \
            str(row['total_amount'])
    print '%s\t%s' % (key, value)

