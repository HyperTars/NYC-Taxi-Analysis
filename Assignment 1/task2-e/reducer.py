#!/usr/bin/env python
import sys

taxi = None
prev_taxi = None
taxi_his = {}


def output():
    count_trips = 0
    for his in taxi_his.values():
        count_trips += his
    count_date = len(taxi_his)
    print '%s\t%d,%.2f' % (prev_taxi, count_trips, float(count_trips)/float(count_date))


for line in sys.stdin:
    taxi, date = line.strip().split('\t', 1)

    if taxi == prev_taxi:
        if date in taxi_his.keys():
            taxi_his[date] += 1
        else:
            taxi_his[date] = 1

    else:
        if (prev_taxi):
            output()
        prev_taxi = taxi
        taxi_his = {}
        taxi_his[date] = 1

if prev_taxi == taxi:
    output()

'''
rm -rf MedallionTrips.out
hfs -rm -r MedallionTrips.out
hjs -D mapreduce.job.reduces=1 \
-file ~/Task2-e/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/MedallionTrips.out
hfs -get MedallionTrips.out
hfs -getmerge MedallionTrips.out MedallionTrips.txt
tail MedallionTrips.txt
'''
