import sys
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# file = sc.textFile("task1a.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))

taxi = lines.map(lambda x: x[0]).distinct()
coord = lines.map(lambda x: (x[0], (float(x[10]), float(x[11]), float(x[12]), float(x[13]))))
empty = coord.filter(lambda x: (x[1][0] == 0 or x[1][0] is None) and (x[1][1] == 0 or x[1][1] is None) \
    and (x[1][2] == 0 or x[1][2] is None) and (x[1][3] == 0 or x[1][3] is None))
coord = coord.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
empty = empty.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
result = coord.leftOuterJoin(empty)
result = result.map(lambda x: (x[0], '%.2f' % (float(0 if x[1][1] is None else x[1][1]) / x[1][0] * 100)))
result = result.sortBy(lambda x: x[0])
output = result.map(lambda x: x[0] + ',' + x[1])
output.saveAsTextFile("task3c.out")

sc.stop()

'''
module load python/gnu/3.4.4
module load spark/2.2.0
rm -rf task3c.out
hfs -rm -R task3c.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3c.py task1a.out
hfs -getmerge task3c.out task3c.out
'''
