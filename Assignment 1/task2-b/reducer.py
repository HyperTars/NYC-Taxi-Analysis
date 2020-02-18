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
cd ~/hw1/Task2-b/
rm -rf TripAmountSamp.out
hfs -rm -r TripAmountSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-b/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TripAmountSamp.out
hfs -get TripAmountSamp.out
hfs -getmerge TripAmountSamp.out TripAmountSamp.txt
cat TripAmountSamp.txt
'''


# Run Code
'''
cd ~/hw1/Task2-b/
rm -rf TripAmount.out
hfs -rm -r TripAmount.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-b/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/TripAmount.out
hfs -get TripAmount.out
hfs -getmerge TripAmount.out TripAmount.txt
cat TripAmount.txt
'''
