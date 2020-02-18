import os
import string
import numpy
import csv
filePath = './fares_sunday.csv'
#filePath = os.environ["mapreduce_map_input_file"]
csvFile = open(filePath, 'r', newline='')
d = csv.reader(csvFile)
for line in d:
    print(line)

