#!/usr/bin/env python
import sys
import re
ranges = []
i = 0
while i <= 48:
    lo = i
    hi = i + 4
    cur_range = []
    if lo != 0:
        lo += 0.01
    if hi == 52:
        hi = 999999999
    cur_range.append(lo)
    cur_range.append(hi)
    ranges.append(cur_range)
    i += 4

for line in sys.stdin:
    data = line.strip()
    data = re.split(',|\t', data)
    key = float(data[15])

    for range in ranges:
        if range[0] <= key and key <= range[1]:
            if (range[1] == 999999999):
                print '%s\t%s' % (13, 1)
            else:
                print '%s\t%s' % (str(range[1]/4), 1)
            break
