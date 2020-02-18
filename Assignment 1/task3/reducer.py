#!/usr/bin/env python
from operator import itemgetter
import sys

prev_key = None
tf_val = []
lc_val = []
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
        for tf in tf_val:
            for lc in lc_val:
                print '%s\t%s' % (prev_key, tf + ',' + lc)
        # update
        prev_key = key
        tf_val = []
        lc_val = []

    if s_val[0] == 'tf':
        tf_val.append(','.join(s_val[1:]))
    if s_val[0] == 'lc':
        lc_val.append(','.join(s_val[1:]))
        
for tf in tf_val:
    for lc in lc_val:
        print '%s\t%s' % (prev_key, tf + ',' + lc)
