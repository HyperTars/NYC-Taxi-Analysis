#!/usr/bin/env python
import sys

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)

    # taxi (medallion)
    taxi = key.split(',')[0]

    # date
    date = key.split(',')[3][:10]

    # print
    print '%s\t%s' % (taxi, date)

# Test Sample Dataset Code
'''
cd ~/hw1/task2-e/
rm -rf MedallionTripsSamp
hfs -rm -r MedallionTripsSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task2-e/ \
-mapper task2-e/map.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/MedallionTripsSamp
hfs -get MedallionTripsSamp
hfs -getmerge MedallionTripsSamp MedallionTripsSamp.txt
rm -rf MedallionTripsSamp
cat MedallionTripsSamp.txt
'''
