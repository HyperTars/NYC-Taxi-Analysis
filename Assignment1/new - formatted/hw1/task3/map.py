#!/usr/bin/env python
import sys
import re
for line in sys.stdin:
    # skip invalid
    if len(line) <= 1 or 'medallion' in line:
        continue

    # extract data
    data = line.strip()
    data = re.split(',|\t', data)
    key = data[0]
    val = ','.join(data[1:])

    # print
    if 'MEDALLION' in line or 'CUR' in line:
        print '%s\tlc,%s' % (key, val)
    else:
        print '%s\ttf,%s' % (key, val)

# Test Sample Dataset Code
'''
cd ~/hw1/task3
rm -rf VehicleJoinSamp
hfs -rm -r VehicleJoinSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task3/ \
-mapper task3/map.py \
-input /user/wl2154/TripFareJoinSamp.txt /user/wl2154/licenses_samp.csv \
-output /user/wl2154/VehicleJoinSamp
hfs -get VehicleJoinSamp
hfs -getmerge VehicleJoinSamp VehicleJoinSamp.txt
rm -rf VehicleJoinSamp
cat VehicleJoinSamp.txt
'''
