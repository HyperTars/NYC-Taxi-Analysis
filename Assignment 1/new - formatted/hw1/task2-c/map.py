#!/usr/bin/env python
import sys

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)

    # passenger_count
    try:
        key = int(val.split(',')[3])
    except ValueError:
        continue

    # print
    print '%s\t%s' % (key, 1)

# Test Sample Dataset Code
'''
cd ~/hw1/task2-c/
rm -rf NumberPassengersSamp
hfs -rm -r NumberPassengersSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task2-c/ \
-mapper task2-c/map.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/NumberPassengersSamp
hfs -get NumberPassengersSamp
hfs -getmerge NumberPassengersSamp NumberPassengersSamp.txt
rm -rf NumberPassengersSamp
cat NumberPassengersSamp.txt
'''
