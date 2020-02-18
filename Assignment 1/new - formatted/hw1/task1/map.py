#!/usr/bin/env python
import sys

for line in sys.stdin:
    # skip invalid data
    line = line.strip()
    if len(line) <= 1 or 'medallion' in line:
        continue

    # split data
    data = line.split(',')

    # fares
    if len(data) == 11:
        key = ','.join(data[0:4])
        val = ','.join(data[4:])
        print '%s\tf,%s' % (key, val)

    # trips
    if len(data) == 14:
        key_ = ','.join(data[0:3])
        key = key_ + ',' + data[5]
        val = ','.join(data[3:5] + data[6:])
        print '%s\tt,%s' % (key, val)

# Test Sample Dataset Code
'''
cd ~/hw1/task1/
rm -rf TripFareJoinSamp
hfs -rm -r TripFareJoinSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task1/ \
-mapper task1/map.py \
-input /user/wl2154/fares_samp.csv /user/wl2154/trips_samp.csv \
-output /user/wl2154/TripFareJoinSamp
hfs -get TripFareJoinSamp
hfs -getmerge TripFareJoinSamp TripFareJoinSamp.txt
rm -rf TripFareJoinSamp
cat TripFareJoinSamp.txt
'''
