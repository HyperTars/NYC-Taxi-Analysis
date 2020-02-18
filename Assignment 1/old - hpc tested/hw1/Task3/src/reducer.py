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

# Test Code
'''
cd ~/hw1/Task3
rm -rf VehicleJoinSamp.out
hfs -rm -r VehicleJoinSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task3/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt /user/wl2154/licenses_samp.csv \
-output /user/wl2154/VehicleJoinSamp.out
hfs -get VehicleJoinSamp.out
hfs -getmerge VehicleJoinSamp.out VehicleJoinSamp.txt
cat VehicleJoinSamp.txt
'''

# Run Code
'''
cd ~/hw1/Task3
rm -rf VehicleJoin.out
hfs -rm -r VehicleJoin.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task3/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt /user/wl2154/licenses.csv \
-output /user/wl2154/VehicleJoin.out
hfs -get VehicleJoin.out
hfs -getmerge VehicleJoin.out VehicleJoin.txt
head VehicleJoin.txt
tail VehicleJoin.txt
wc -l VehicleJoin.txt
'''
