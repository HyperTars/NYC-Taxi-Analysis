#!/usr/bin/env python
import sys

date = None
prev_date = None
total_revenue = 0.00
total_tolls = 0.00

for line in sys.stdin:
    # extract data
    date, val = line.strip().split('\t', 1)
    revenue, tolls = val.split(',', 1)
    try:
        float_revenue = float(revenue)
        float_tolls = float(tolls)
    except ValueError:
        continue

    # key (date) exists, accumulate
    if prev_date == date:
        total_revenue += float_revenue
        total_tolls += float_tolls

    # new key
    else:
        # print previous key pair
        if prev_date:
            print '%s\t%.2f,%.2f' % (prev_date, total_revenue, total_tolls)

        # update
        prev_date = date
        total_revenue = float_revenue
        total_tolls = float_tolls

# print last key pair
if prev_date == date:
    print '%s\t%.2f,%.2f' % (prev_date, total_revenue, total_tolls)

# Test Sample Dataset Code
'''
cd ~/hw1/task2-d/
rm -rf TotalRevenueSamp
hfs -rm -r TotalRevenueSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-d/ \
-mapper task2-d/map.py \
-reducer task2-d/reduce.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TotalRevenueSamp
hfs -get TotalRevenueSamp
hfs -getmerge TotalRevenueSamp TotalRevenueSamp.txt
rm -rf TotalRevenueSamp
cat TotalRevenueSamp.txt
'''

# Run Complete Dataset Code
'''
cd ~/hw1/task2-d/
rm -rf TotalRevenue
hfs -rm -r TotalRevenue
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-d/ \
-mapper task2-d/map.py \
-reducer task2-d/reduce.py \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/TotalRevenue
hfs -get TotalRevenue
hfs -getmerge TotalRevenue TotalRevenue.txt
cat TotalRevenue.txt
'''
