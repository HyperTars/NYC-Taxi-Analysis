import sys
from csv import reader
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

#file_fares = sc.textFile("fares_samp.csv")
#file_trips = sc.textFile("trips_samp.csv")

file_trips = sc.textFile(sys.argv[1], 1)
file_fares = sc.textFile(sys.argv[2], 1)

line_trips = file_trips.mapPartitions(lambda x: reader(x)). \
        filter(lambda line: len(line) > 1 and 'medallion' not in line)
line_fares = file_fares.mapPartitions(lambda x: reader(x)). \
        filter(lambda line: len(line) > 1 and 'medallion' not in line)

map_fares = line_fares.map(lambda x: ((x[0], x[1], x[2], x[3]), \
        (x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
map_trips = line_trips.map(lambda x: ((x[0], x[1], x[2], x[5]), \
        (x[3], x[4], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13])))

result = map_trips.join(map_fares)
result = result.sortBy(lambda x: (x[0][0], x[0][1], x[0][3]))

output = result.map(lambda x: ','.join(x[0]) + ',' + \
        ','.join(x[1][0]) + ',' + ','.join(x[1][1]))
output.saveAsTextFile("task1a.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task1a.out
hfs -rm -R task1a.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task1a.py /user/hc2660/hw2data/Trips.csv \
/user/hc2660/hw2data/Fares.csv
hfs -getmerge task1a.out task1a.out
hfs -rm -R task1a.out
hfs -put task1a.out
'''
