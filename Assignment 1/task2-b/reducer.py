#!/usr/bin/env python
import sys

count = 0
prev_key = ''
sel_trips = []

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)

    # skip trip(key) already counted
    if key == prev_key and prev_key != '':
        continue

    try:
        val = float(val)
    except ValueError:
        continue

    if val <= 10.00:
        count += 1
        # test
        prev_key = key
        print '%s\t%s\t%s' % (key, val, count)

print count

'''
rm -rf TripAmount.out
hfs -rm -r TripAmount.out
hjs -D mapreduce.job.reduces=1 \
-file ~/Task2-b/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/TripAmount.out
hfs -get TripAmount.out
hfs -getmerge TripAmount.out TripAmount.txt

'''
