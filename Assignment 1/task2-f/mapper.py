#!/usr/bin/env python
import os
from operator import itemgetter
import sys

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)
    s_key = key.split(',')
    s_val = val.split(',')
    med = s_key[0]
    lic = s_key[1]
    print '%s\t%s' % (lic, med)

'''
rm -rf UniqueTaxis.out
hfs -rm -r UniqueTaxis.out
hjs -D mapreduce.job.reduces=0 \
-file ~/Task2-f/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/UniqueTaxis.out
hfs -get UniqueTaxis.out
hfs -getmerge UniqueTaxis.out UniqueTaxis.txt
cat UniqueTaxis.txt
'''
