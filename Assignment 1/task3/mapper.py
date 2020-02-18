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

    if 'MEDALLION' in line or 'CUR' in line:
        print '%s\tlc,%s' % (key, val)
    else:
        print '%s\ttf,%s' % (key, val)

# Test Code
'''
cd ~/hw1/Task3
rm -rf VehicleJoinSamp.out
hfs -rm -r VehicleJoinSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task3/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt /user/wl2154/licenses_samp.csv \
-output /user/wl2154/VehicleJoinSamp.out
hfs -get VehicleJoinSamp.out
hfs -getmerge VehicleJoinSamp.out VehicleJoinSamp.txt
cat VehicleJoinSamp.txt
'''
