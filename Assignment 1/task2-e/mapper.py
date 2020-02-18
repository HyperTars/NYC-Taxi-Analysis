#!/usr/bin/env python
import sys

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)
    s_key = key.split(',')
    s_val = val.split(',')
    taxi = s_key[0]
    date = s_key[3]
    date = date[:10]
    print '%s\t%s' % (taxi, date)

'''
rm -rf MedallionTrips.out
hfs -rm -r MedallionTrips.out
hjs -D mapreduce.job.reduces=0 \
-file ~/Task2-e/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/MedallionTrips.out
hfs -get MedallionTrips.out
hfs -getmerge MedallionTrips.out MedallionTrips.txt
cat MedallionTrips.txt
'''
