#!/usr/bin/env python
import sys

lic = None
prev_lic = None
lic_med = []

for line in sys.stdin:
    # extract data
    lic, med = line.strip().split('\t', 1)

    # key (license aka driver) exists, accumulate
    if lic == prev_lic:
        # medallion (taxi) exists
        if med in lic_med:
            continue

        # new medallion (taxi)
        else:
            lic_med.append(med)

    # new key
    else:
        # print previous key pair
        if (prev_lic):
            print '%s\t%s' % (prev_lic, len(lic_med))

        # update
        prev_lic = lic
        lic_med = []
        lic_med.append(med)

# print last key pair
if prev_lic == lic:
    print '%s\t%s' % (prev_lic, len(lic_med))

# Test Code
'''
cd ~/hw1/task2-f/
rm -rf UniqueTaxisSamp
hfs -rm -r UniqueTaxisSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-f/ \
-mapper task2-f/map.py \
-reducer task2-f/reduce.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/UniqueTaxisSamp
hfs -get UniqueTaxisSamp
hfs -getmerge UniqueTaxisSamp UniqueTaxisSamp.txt
rm -rf UniqueTaxisSamp
cat UniqueTaxisSamp.txt
'''

# Run Code
'''
cd ~/hw1/task2-f/
rm -rf UniqueTaxis
hfs -rm -r UniqueTaxis
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-f/ \
-mapper task2-f/map.py \
-reducer task2-f/reduce.py \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/UniqueTaxis
hfs -get UniqueTaxis
hfs -getmerge UniqueTaxis UniqueTaxis.txt
tail UniqueTaxis.txt
wc -l UniqueTaxis.txt
'''
