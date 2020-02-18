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

# Test Code
'''
cd ~/hw1/Task1
rm -rf TripFareJoinSamp.out
hfs -rm -r TripFareJoinSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task1/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/fares_samp.csv /user/wl2154/trips_samp.csv \
-output /user/wl2154/TripFareJoinSamp.out
hfs -get TripFareJoinSamp.out
hfs -getmerge TripFareJoinSamp.out TripFareJoinSamp.txt
cat TripFareJoinSamp.txt
'''

# Run Code
'''
cd ~/hw1/Task1
rm -rf TripFareJoin.out
hfs -rm -r TripFareJoin.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task1/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/fare_data.csv /user/wl2154/trip_data.csv \
-output /user/wl2154/TripFareJoin.out
hfs -get TripFareJoin.out
hfs -getmerge TripFareJoin.out TripFareJoin.txt
hfs -rm TripFareJoin.txt
hfs -put TripFareJoin.txt
head TripFareJoin.txt
tail TripFareJoin.txt
wc -l TripFareJoin.txt
'''
