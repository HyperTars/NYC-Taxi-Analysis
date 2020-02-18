#!/usr/bin/env python
from operator import itemgetter
import sys

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)
    s_val = val.split(',')
    val = s_val[4]
    print '%s\t%s' % (key, val)

'''
rm -rf NumberPassengers.out
hfs -rm -r NumberPassengers.out
hjs -D mapreduce.job.reduces=0 \
-file ~/Task2-c/src/ \
-mapper src/mapper.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/NumberPassengers.out
hfs -get NumberPassengers.out
hfs -getmerge NumberPassengers.out NumberPassengers.txt
'''