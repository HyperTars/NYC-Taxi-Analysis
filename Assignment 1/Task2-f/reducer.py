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
cd ~/hw1/Task2-f/
rm -rf UniqueTaxisSamp.out
hfs -rm -r UniqueTaxisSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-f/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/UniqueTaxisSamp.out
hfs -get UniqueTaxisSamp.out
hfs -getmerge UniqueTaxisSamp.out UniqueTaxisSamp.txt
cat UniqueTaxisSamp.txt
'''

# Run Code
'''
cd ~/hw1/Task2-f/
rm -rf UniqueTaxis.out
hfs -rm -r UniqueTaxis.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-f/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/UniqueTaxis.out
hfs -get UniqueTaxis.out
hfs -getmerge UniqueTaxis.out UniqueTaxis.txt
tail UniqueTaxis.txt
wc -l UniqueTaxis.txt
'''
