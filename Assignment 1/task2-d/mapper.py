#!/usr/bin/env python
import sys

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)
    s_val = val.split(',')

    # day
    day = key.split(',')[3][:10]

    # revenue
    try:
        revenue = float(s_val[11]) + float(s_val[12]) + float(s_val[14])
    except ValueError:
        continue

    # tolls
    try:
        tolls = float(s_val[15])
    except ValueError:
        continue

    # print
    print '%s\t%s,%s' % (day, revenue, tolls)

'''
cd ~/hw1/Task2-d
rm -rf TotalRevenueSamp.out
hfs -rm -r TotalRevenueSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task2-d/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TotalRevenueSamp.out
hfs -get TotalRevenueSamp.out
hfs -getmerge TotalRevenueSamp.out TotalRevenueSamp.txt
cat TotalRevenueSamp.txt
'''
