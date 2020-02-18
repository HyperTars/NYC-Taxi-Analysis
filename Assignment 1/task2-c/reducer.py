#!/usr/bin/env python
import sys

prev_key = ''
prev_val = ''
count = []
i = 0
while i <= 20:
    count.append(0)
    i += 1

for line in sys.stdin:
    key, val = line.strip().split('\t', 1)

    # skip trip(key) already counted
    # if key == prev_key and prev_key != '':
    #    continue
    # skip trip (key & val) already counted
    if prev_key != '' and prev_val != '':
        if key == prev_key and val == prev_val:
            continue

    try:
        val = int(val)
    except ValueError:
        continue

    count[val] += 1
    prev_key = key
    prev_val = val

j = 20
max = 20
while count[j] == 0:
    max = j
    j -= 1

for idx in range(1, max):
    print '%s\t%s' % (idx, count[idx])

total = 0
for cnt in count:
    total += cnt

print '%s\t%s' % ('total', total)

'''
rm -rf NumberPassengers.out
hfs -rm -r NumberPassengers.out
hjs -D mapreduce.job.reduces=1 \
-file ~/Task2-c/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/NumberPassengers.out
hfs -get NumberPassengers.out
hfs -getmerge NumberPassengers.out NumberPassengers.txt
cat NumberPassengers.txt
'''
