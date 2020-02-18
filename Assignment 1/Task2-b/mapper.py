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

# Test Code
'''
cd ~/hw1/Task2-b
rm -rf TripAmountSamp.out
hfs -rm -r TripAmountSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task2-b/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TripAmountSamp.out
hfs -get TripAmountSamp.out
hfs -getmerge TripAmountSamp.out TripAmountSamp.txt
cat TripAmountSamp.txt
'''

