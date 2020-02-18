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
rm -rf FareAmountSamp.out
hfs -rm -r FareAmountSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task2-a/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/FareAmountSamp.out
hfs -get FareAmountSamp.out
hfs -getmerge FareAmountSamp.out FareAmountSamp.txt
cat FareAmountSamp.txt
'''
