#!/usr/bin/env python
import sys

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)

    # medallion and licenses
    med = key.split(',')[0]
    lic = key.split(',')[1]

    # print
    print '%s\t%s' % (lic, med)

'''
cd ~/hw1/task2-f/
rm -rf UniqueTaxisSamp
hfs -rm -r UniqueTaxisSamp
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/task2-f/ \
-mapper task2-f/map.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/UniqueTaxisSamp
hfs -get UniqueTaxisSamp
hfs -getmerge UniqueTaxisSamp UniqueTaxisSamp.txt
cat UniqueTaxisSamp.txt
'''
