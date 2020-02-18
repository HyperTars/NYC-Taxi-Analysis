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

# Test Code
'''
cd ~/hw1/Task2-e/
rm -rf MedallionTripsSamp.out
hfs -rm -r MedallionTripsSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/Task2-e/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/MedallionTripsSamp.out
hfs -get MedallionTripsSamp.out
hfs -getmerge MedallionTripsSamp.out MedallionTripsSamp.txt
cat MedallionTripsSamp.txt
'''

# Run Code
'''
cd ~/hw1/Task2-e/
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
wc -l MedallionTrips.txt
'''
