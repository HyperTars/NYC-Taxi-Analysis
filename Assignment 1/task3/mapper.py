#!/usr/bin/env python
import os
import string
import sys
import re
for line in sys.stdin:
        if len(line) <= 1:
                continue
        data = line.strip()
        data = re.split(',|\t', data)
        key = data[0]
        if key == 'medallion':
                continue
        val = ','.join(data[1:])
        if 'MEDALLION' in line or 'medallion' in line or 'CUR' in line or 'cur' in line:
                print '%s\tlc,%s' % (key, val)
        else:
                print '%s\ttf,%s' % (key, val)

