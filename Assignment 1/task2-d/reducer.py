#!/usr/bin/env python
from operator import itemgetter
import sys

date = None
curr_date = None
total_revenue = 0.00
total_tolls = 0.00

for line in sys.stdin:
    date, val = line.strip().split('\t', 1)
    revenue, tolls = val.split(',', 1)

    try:
        float_revenue = float(revenue)
        float_tolls = float(tolls)
    except ValueError:
        continue

    if curr_date == date:
        total_revenue += float_revenue
        total_tolls += float_tolls
    else:
        if curr_date:
            print '%s\t%.2f,%.2f' % (curr_date, total_revenue, total_tolls)
        curr_date = date
        total_revenue = float_revenue
        total_tolls = float_tolls

if curr_date == date:
    print '%s\t%.2f,%.2f' % (curr_date, total_revenue, total_tolls)

'''
rm -rf TotalRevenue.out
hfs -rm -r TotalRevenue.out
hjs -D mapreduce.job.reduces=1 \
-file ~/Task2-d/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/TotalRevenue.out
hfs -get TotalRevenue.out
hfs -getmerge TotalRevenue.out TotalRevenue.txt
cat TotalRevenue.txt
'''
