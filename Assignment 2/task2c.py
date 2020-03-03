import sys
from pyspark import SparkContext
from itertools import accumulate


def calsum(values):
    return [[k[0], k[1]] for k in zip(accumulate([i[0] for i in values]), \
            accumulate([i[1] for i in values]))]


sc = SparkContext.getOrCreate()
# file = sc.textFile("AllTripsSamp.txt")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
data = lines.map(lambda x: ((x[3][:10]), (float(x[15]) + float(x[16]) + \
        float(x[18]), float(x[19]))))
result = data.groupByKey().mapValues(calsum) \
        .map(lambda x: (x[0], '%.2f' % x[1][-1][0], '%.2f' % x[1][-1][1]))
output = result.sortBy(lambda x: x[0]) \
        .map(lambda x: x[0] + ',' + x[1] + ',' + x[2])
output.saveAsTextFile("task2c.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2c.out
hfs -rm -R task2c.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2c.py task1a.out
hfs -getmerge task2c.out task2c.out
'''
