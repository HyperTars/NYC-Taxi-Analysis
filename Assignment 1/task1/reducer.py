#!/usr/bin/env python
from operator import itemgetter
import sys

prev_key = None
trips_val = []
fares_val = []
key = None
val = None
for line in sys.stdin:
    line = line.strip()
    key, val = line.split('\t', 1)
    s_val = val.split(',')

    if key is None:
        continue

    if key != prev_key:
        # print
        for trips in trips_val:
            for fares in fares_val:
                print '%s\t%s' % (prev_key, trips + ',' + fares)
        # update
        prev_key = key
        trips_val = []
        fares_val = []

    if len(s_val) == 7:
        fares_val.append(val)
    if len(s_val) == 10:
        trips_val.append(val)


for trips in trips_val:
    for fares in fares_val:
        print '%s\t%s' % (prev_key, trips + ',' + fares)
