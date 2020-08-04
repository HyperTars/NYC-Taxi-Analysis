#!/usr/bin/env python
import sys

# construct range list
ranges = []
i = 0
while i <= 48:
    if i == 0:
        ranges.append([0, 4])
    elif i == 48:
        ranges.append([48.01, 999999999])
    else:
        ranges.append([i + 0.01, i + 4])
    i += 4

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)
    s_val = val.split(',')

    # fare_amount
    try:
        key = float(s_val[11])
    except ValueError:
        continue

    # classify and print
    for range in ranges:
        if range[0] <= key and key <= range[1]:
            if (range[0] == 48.01):
                # last range
                print '%s\t%s' % (13, 1)
            else:
                # other ranges
                print '%s\t%s' % (str(range[1]/4), 1)
            break

# Test Sample Dataset Code
'''
cd ~/hw1/task2-a
rm -rf FareAmountSamp
hfs -rm -r FareAmountSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task2-a/ \
-mapper task2-a/map.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/FareAmountSamp
hfs -get FareAmountSamp
hfs -getmerge FareAmountSamp FareAmountSamp.txt
rm -rf FareAmountSamp
cat FareAmountSamp.txt
'''
