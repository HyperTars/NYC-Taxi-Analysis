#!/usr/bin/env python
import sys

# construct range list
i = 1
count = []
while i <= 14:
    count.append(0)
    i += 1

for line in sys.stdin:
    # extract data
    key, val = line.strip().split('\t', 1)

    # fare_amount
    try:
        int_key = int(key)
    except ValueError:
        continue

    # accumulate
    count[int_key] += 1

# print
print '%s\t%s' % ('0,     4', count[1])
print '%s\t%s' % ('4.01,  8', count[2])
print '%s\t%s' % ('8.01,  12', count[3])
print '%s\t%s' % ('12.01, 16', count[4])
print '%s\t%s' % ('16.01, 20', count[5])
print '%s\t%s' % ('20.01, 24', count[6])
print '%s\t%s' % ('24.01, 28', count[7])
print '%s\t%s' % ('28.01, 32', count[8])
print '%s\t%s' % ('32.01, 36', count[9])
print '%s\t%s' % ('36.01, 40', count[10])
print '%s\t%s' % ('40.01, 44', count[11])
print '%s\t%s' % ('44.01, 48', count[12])
print '%s\t%s' % ('48.01, infinite', count[13])

# Test Code
'''
cd ~/hw1/Task2-a/
rm -rf FareAmountSamp.out
hfs -rm -r FareAmountSamp.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-a/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoinSamp.txt \
-output /user/wl2154/FareAmountSamp.out
hfs -get FareAmountSamp.out
hfs -getmerge FareAmountSamp.out FareAmountSamp.txt
cat FareAmountSamp.txt
'''


# Run Code
'''
cd ~/hw1/Task2-a/
rm -rf FareAmount.out
hfs -rm -r FareAmount.out
hjs -D mapreduce.job.reduces=1 \
-file ~/hw1/Task2-a/src/ \
-mapper src/mapper.sh \
-reducer src/reducer.sh \
-input /user/wl2154/TripFareJoin.txt \
-output /user/wl2154/FareAmount.out
hfs -get FareAmount.out
hfs -getmerge FareAmount.out FareAmount.txt
cat FareAmount.txt
'''
