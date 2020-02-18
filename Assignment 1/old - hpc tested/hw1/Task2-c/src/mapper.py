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

'''
cd ~/hw1/Task2-c/
rm -rf NumberPassengersSamp.out
hfs -rm -r NumberPassengersSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task2-c/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/NumberPassengersSamp.out
hfs -get NumberPassengersSamp.out
hfs -getmerge NumberPassengersSamp.out NumberPassengersSamp.txt
cat NumberPassengersSamp.txt
'''
