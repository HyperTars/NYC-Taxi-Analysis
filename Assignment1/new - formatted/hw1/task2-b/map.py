#!/usr/bin/env python
import sys

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)
    s_val = val.split(',')

    # total_amount
    try:
        key = float(s_val[-1])
    except ValueError:
        continue

    # print less than or equal to $10
    if key <= 10.00:
        print '%s\t%s' % (key, 1)

# Test Sample Dataset Code
'''
cd ~/hw1/task2-b
rm -rf TripAmountSamp
hfs -rm -r TripAmountSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task2-b/ \
-mapper task2-b/map.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TripAmountSamp
hfs -get TripAmountSamp
hfs -getmerge TripAmountSamp TripAmountSamp.txt
rm -rf TripAmountSamp
cat TripAmountSamp.txt
'''
