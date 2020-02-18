#!/usr/bin/env python
import os
from operator import itemgetter
import sys

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)
    s_val = val.split(',')
    val = s_val[-1]
    print '%s\t%s' % (key, val)
