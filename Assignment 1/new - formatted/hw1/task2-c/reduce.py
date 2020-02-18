#!/usr/bin/env python
import sys

# construct count list
count = []
i = 0
while i <= 50:
    count.append(0)
    i += 1

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)

    # passenger_count
    try:
        key = int(key)
    except ValueError:
        continue

    # accumulate
    count[key] += 1

# delete redundant data
j = 50
max = 50
while count[j] == 0:
    max = j
    j -= 1

# print
for idx in range(1, max):
    print '%s\t%s' % (idx, count[idx])

total = 0
for cnt in count:
    total += cnt

print '%s\t%s' % ('total', total)

# Test Sample Dataset Code
'''
cd ~/hw1/task2-c/
rm -rf NumberPassengersSamp
hfs -rm -r NumberPassengersSamp
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-c/ \
-mapper task2-c/map.py \
-reducer task2-c/reduce.py \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/NumberPassengersSamp
hfs -get NumberPassengersSamp
hfs -getmerge NumberPassengersSamp NumberPassengersSamp.txt
rm -rf NumberPassengersSamp
cat NumberPassengersSamp.txt
'''

# Run Complete Dataset Code
'''
cd ~/hw1/task2-c/
rm -rf NumberPassengers
hfs -rm -r NumberPassengers
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/task2-c/ \
-mapper task2-c/map.py \
-reducer task2-c/reduce.py \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/NumberPassengers
hfs -get NumberPassengers
hfs -getmerge NumberPassengers NumberPassengers.txt
cat NumberPassengers.txt
'''
