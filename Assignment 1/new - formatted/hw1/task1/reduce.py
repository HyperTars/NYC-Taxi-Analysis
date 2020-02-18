#!/usr/bin/env python
import sys

prev_key = None
key = None
trips_val = []
fares_val = []


def output():
    for trips in trips_val:
        for fares in fares_val:
            print '%s\t%s' % (prev_key, trips + ',' + fares)


for line in sys.stdin:
    # extract
    key, val = line.strip().split('\t', 1)
    tag = val.split(',')[0]
    val = ','.join(val.split(',')[1:])

    # key exists, add pair
    if key == prev_key and prev_key is not None:
        if tag == 'f':
            fares_val.append(val)
        if tag == 't':
            trips_val.append(val)

    # new key
    else:
        # print previous key pair
        if (prev_key):
            output()

        # update
        prev_key = key
        trips_val = []
        fares_val = []
        if tag == 'f':
            fares_val.append(val)
        if tag == 't':
            trips_val.append(val)

# print last key pair
if prev_key == key:
    output()

# Test Sample Dataset Code
'''
cd ~/hw1/task1
rm -rf TripFareJoinSamp
hfs -rm -r TripFareJoinSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task1/ \
-mapper task1/map.py \
-reducer task1/reduce.py \
-input /user/wl2154/fares_samp.csv /user/wl2154/trips_samp.csv \
-output /user/wl2154/TripFareJoinSamp
hfs -get TripFareJoinSamp
hfs -getmerge TripFareJoinSamp TripFareJoinSamp.txt
rm -rf TripFareJoinSamp
cat TripFareJoinSamp.txt
'''

# Run Complete Dataset Code
'''
cd ~/hw1/task1
rm -rf TripFareJoin
hfs -rm -r TripFareJoin
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task1/ \
-mapper task1/map.py \
-reducer task1/reduce.py \
-input /user/wl2154/fare_data.csv /user/wl2154/trip_data.csv \
-output /user/wl2154/TripFareJoin
hfs -get TripFareJoin
hfs -getmerge TripFareJoin TripFareJoin.txt
hfs -rm TripFareJoin.txt
hfs -put TripFareJoin.txt
head TripFareJoin.txt
tail TripFareJoin.txt
wc -l TripFareJoin.txt
'''
