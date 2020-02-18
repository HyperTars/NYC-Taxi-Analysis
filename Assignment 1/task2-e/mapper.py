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

# Test Code
'''
cd ~/hw1/Task2-e/
rm -rf MedallionTripsSamp.out
hfs -rm -r MedallionTripsSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task2-e/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/MedallionTripsSamp.out
hfs -get MedallionTripsSamp.out
hfs -getmerge MedallionTripsSamp.out MedallionTripsSamp.txt
cat MedallionTripsSamp.txt
'''
