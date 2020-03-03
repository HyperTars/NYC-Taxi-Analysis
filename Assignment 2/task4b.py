import sys
from pyspark import SparkContext
from itertools import accumulate


def calsum(values):
    return [[k[0], k[1], k[2]] for k in zip(accumulate([i[0] for i in values]), \
            accumulate([i[1] for i in values]), \
            accumulate([i[2] for i in values]))]


sc = SparkContext.getOrCreate()
# file = sc.textFile("task1b.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(',MEDALLION,CUR,')) \
        .map(lambda x: ((x[0].split(',')), (x[1].split(','))))
trips = lines.map(lambda x: (x[1][4], (float(x[0][5]), float(x[0][8]), 1)))

result = trips.groupByKey().mapValues(calsum) \
        .map(lambda x: (x[0], x[1][-1][0], x[1][-1][1] / x[1][-1][0] * 100, x[1][-1][2])) \
        .map(lambda x: (x[0], '%d' % x[3], '%.2f' % x[1], '%.2f' % x[2]))
output = result.sortBy(lambda x: x[0]) \
        .map(lambda x: x[0] + ',' + x[1] + ',' + x[2] + ',' + x[3])
output.saveAsTextFile("task4b.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task4b.out
hfs -rm -R task4b.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task4b.py task1b.out
hfs -getmerge task4b.out task4b.out
'''
