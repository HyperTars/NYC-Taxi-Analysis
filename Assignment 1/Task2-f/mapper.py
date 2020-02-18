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
cd ~/hw1/Task2-f/
rm -rf UniqueTaxisSamp.out
hfs -rm -r UniqueTaxisSamp.out
hjs -D mapreduce.job.reduces=0 \
-file ~/hw1/Task2-f/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/UniqueTaxisSamp.out
hfs -get UniqueTaxisSamp.out
hfs -getmerge UniqueTaxisSamp.out UniqueTaxisSamp.txt
cat UniqueTaxisSamp.txt
'''
