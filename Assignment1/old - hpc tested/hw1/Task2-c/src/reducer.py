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

# Test Code
'''
cd ~/hw1/Task2-c/
rm -rf NumberPassengersSamp.out
hfs -rm -r NumberPassengersSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-c/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/NumberPassengersSamp.out
hfs -get NumberPassengersSamp.out
hfs -getmerge NumberPassengersSamp.out NumberPassengersSamp.txt
cat NumberPassengersSamp.txt
'''

# Run Code
'''
cd ~/hw1/Task2-c/
rm -rf NumberPassengers.out
hfs -rm -r NumberPassengers.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-c/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/NumberPassengers.out
hfs -get NumberPassengers.out
hfs -getmerge NumberPassengers.out NumberPassengers.txt
cat NumberPassengers.txt
'''
