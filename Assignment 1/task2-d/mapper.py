#!/usr/bin/env python
import sys

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)
    s_key = key.split(',')
    s_val = val.split(',')
    day = s_key[3]
    day = day[:10]
    revenue = float(s_val[11]) + float(s_val[12]) + float(s_val[14])
    tolls = float(s_val[15])
    print '%s\t%s,%s' % (day, revenue, tolls)

'''
rm -rf TotalRevenue.out
hfs -rm -r TotalRevenue.out
hjs -D mapreduce.job.reduces=0 \
-file ~/Task2-d/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TotalRevenue.out
hfs -get TotalRevenue.out
hfs -getmerge TotalRevenue.out TotalRevenue.txt
cat TotalRevenue.txt
'''
