from operator import itemgetter
import sys

prev_key = None
prev_val = None
key = None
val = None
curr_pair = []
for line in sys.stdin:
    line = line.strip()
    key, val = line.split('\t', 1)

    if key != prev_key and prev_key is not None:
        for pair in curr_pair:
            print '%s\t%s' % (pair[0], pair[1])
        curr_pair = []
    else:
        if key:
            prev_key = key
            temp_pair = []
            temp_pair.append(key)
            temp_pair.append(val)
            curr_pair.append(key, val)
for pair in curr_pair:
    print '%s\t%s' % (pair[0], pair[1])
