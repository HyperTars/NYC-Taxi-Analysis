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
    avg_trips = float(count_trips) / float(count_date)
    print '%s\t%d,%.2f' % (prev_taxi, count_trips, avg_trips)


for line in sys.stdin:
    # extract data
    taxi, date = line.strip().split('\t', 1)

    # key (taxi) exists, accumulate
    if taxi == prev_taxi:
        # date exists
        if date in taxi_his.keys():
            taxi_his[date] += 1

        # new date
        else:
            taxi_his[date] = 1

    # new key
    else:
        # print previous key pair
        if (prev_taxi):
            output()

        # update
        prev_taxi = taxi
        taxi_his = {}
        taxi_his[date] = 1

# print last key pair
if prev_taxi == taxi:
    output()

# Test Sample Dataset Code
'''
cd ~/hw1/task2-e/
rm -rf MedallionTripsSamp
hfs -rm -r MedallionTripsSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-e/ \
-mapper task2-e/map.py \
-reducer task2-e/reduce.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/MedallionTripsSamp
hfs -get MedallionTripsSamp
hfs -getmerge MedallionTripsSamp MedallionTripsSamp.txt
rm -rf MedallionTripsSamp
cat MedallionTripsSamp.txt
'''

# Run Complete Dataset Code
'''
cd ~/hw1/task2-e/
rm -rf MedallionTrips
hfs -rm -r MedallionTrips
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-e/ \
-mapper task2-e/map.py \
-reducer task2-e/reduce.py \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/MedallionTrips
hfs -get MedallionTrips
hfs -getmerge MedallionTrips MedallionTrips.txt
tail MedallionTrips.txt
wc -l MedallionTrips.txt
'''
