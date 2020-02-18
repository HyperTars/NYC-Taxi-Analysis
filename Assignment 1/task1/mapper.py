#!/usr/bin/env python
import os
import string
import sys

cnt = 0
for line in sys.stdin:
        if len(line) <= 1:
                contiue

        data = line.strip().split(',')

        if data[0] == 'medallion':
                continue

        # fares
        if len(data) == 11:
                key = data[0] + ',' + data[1] + ',' + data[2] + ',' + data[3]
                val = data[4] + ',' + \
                        data[5] + ',' + \
                        data[6] + ',' + \
                        data[7] + ',' + \
                        data[8] + ',' + \
                        data[9] + ',' + \
                        data[10]
                print '%s\t%s' % (key, val)
                
        # trips
        if len(data) == 14:
                key = data[0] + ',' + data[1] + ',' + data[2] + ',' + data[5]
                val = data[3] + ',' + \
                        data[4] + ',' + \
                        data[6] + ',' + \
                        data[7] + ',' + \
                        data[8] + ',' + \
                        data[9] + ',' + \
                        data[10] + ',' + \
                        data[11] + ',' + \
                        data[12] + ',' + \
                        data[13]
                print '%s\t%s' % (key, val)
