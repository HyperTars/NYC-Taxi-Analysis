#!/usr/bin/env python
from operator import itemgetter
import sys

lic = None
prev_lic = None
lic_med = []

for line in sys.stdin:
    lic, med = line.strip().split('\t', 1)

    if lic == prev_lic:
        if med in lic_med:
            continue
        else:
            lic_med.append(med)

    else:
        if (prev_lic):
            a = 1
            print '%s\t%s' % (prev_lic, len(lic_med))
        prev_lic = lic
        lic_med = []
        lic_med.append(med)

if prev_lic == lic:
    print '%s\t%s' % (prev_lic, len(lic_med))
'''
rm -rf UniqueTaxis.out
hfs -rm -r UniqueTaxis.out
hjs -D mapreduce.job.reduces=1 \
-file ~/Task2-f/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/UniqueTaxis.out
hfs -get UniqueTaxis.out
hfs -getmerge UniqueTaxis.out UniqueTaxis.txt
tail UniqueTaxis.txt
'''
