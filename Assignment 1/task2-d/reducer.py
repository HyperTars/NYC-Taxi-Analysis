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

    # key exists, accumulate
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

# Test Code
'''
cd ~/hw1/Task2-d/
rm -rf TotalRevenueSamp.out
hfs -rm -r TotalRevenueSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-d/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TotalRevenueSamp.out
hfs -get TotalRevenueSamp.out
hfs -getmerge TotalRevenueSamp.out TotalRevenueSamp.txt
cat TotalRevenueSamp.txt
'''

# Run Code
'''
cd ~/hw1/Task2-d/
rm -rf TotalRevenue.out
hfs -rm -r TotalRevenue.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-d/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/TotalRevenue.out
hfs -get TotalRevenue.out
hfs -getmerge TotalRevenue.out TotalRevenue.txt
cat TotalRevenue.txt
'''
