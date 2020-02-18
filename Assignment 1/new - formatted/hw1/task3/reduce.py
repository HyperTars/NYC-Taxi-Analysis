#!/usr/bin/env python
import sys

prev_key = None
key = None
tf_val = []
lc_val = []


def output():
    for tf in tf_val:
        for lc in lc_val:
            print '%s\t%s' % (prev_key, tf + ',' + lc)


for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)
    tag = val.split(',')[0]
    val = ','.join(val.split(',')[1:])

    # key exists, add pair
    if key == prev_key and prev_key is not None:
        if tag == 'tf':
            tf_val.append(val)
        if tag == 'lc':
            lc_val.append(val)

    # new key
    else:
        # print previous key pair
        if (prev_key):
            output()

        # update
        prev_key = key
        tf_val = []
        lc_val = []
        if tag == 'tf':
            tf_val.append(val)
        if tag == 'lc':
            lc_val.append(val)

# print last key pair
if prev_key == key:
    output()

# Test Sample Dataset Code
'''
cd ~/hw1/task3
rm -rf VehicleJoinSamp
hfs -rm -r VehicleJoinSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task3 \
-mapper task3/map.py \
-reducer task3/reduce.py \
-input /user/wl2154/TripFareJoinSamp.txt /user/wl2154/licenses_samp.csv \
-output /user/wl2154/VehicleJoinSamp
hfs -get VehicleJoinSamp
hfs -getmerge VehicleJoinSamp VehicleJoinSamp.txt
cat VehicleJoinSamp.txt
'''

# Run Complete Dataset Code
'''
cd ~/hw1/task3
rm -rf VehicleJoin
hfs -rm -r VehicleJoin
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task3 \
-mapper task3/map.py \
-reducer task3/reduce.py \
-input /user/wl2154/TripFareJoin.txt /user/wl2154/licenses.csv \
-output /user/wl2154/VehicleJoin
hfs -get VehicleJoin
hfs -getmerge VehicleJoin VehicleJoin.txt
head VehicleJoin.txt
tail VehicleJoin.txt
wc -l VehicleJoin.txt
'''
