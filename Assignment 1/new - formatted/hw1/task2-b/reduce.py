#!/usr/bin/env python
import sys

count = 0

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)

    # total_amount
    try:
        key = float(key)
    except ValueError:
        continue

    # accumulate
    count += 1

# print
print count

# Test Code
'''
cd ~/hw1/task2-b/
rm -rf TripAmountSamp
hfs -rm -r TripAmountSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-b/ \
-mapper task2-b/map.py \
-reducer task2-b/reduce.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TripAmountSamp
hfs -get TripAmountSamp
hfs -getmerge TripAmountSamp TripAmountSamp.txt
rm -rf TripAmountSamp
cat TripAmountSamp.txt
'''


# Run Code
'''
cd ~/hw1/task2-b/
rm -rf TripAmount
hfs -rm -r TripAmount
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-b/ \
-mapper task2-b/map.py \
-reducer task2-b/reduce.py \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/TripAmount
hfs -get TripAmount
hfs -getmerge TripAmount TripAmount.txt
cat TripAmount.txt
'''
