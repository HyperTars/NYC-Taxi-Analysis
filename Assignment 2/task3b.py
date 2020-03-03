import sys
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# file = sc.textFile("task1a.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
taxi = lines.map(lambda x: ((x[0], x[3]), 1))
tx = taxi.reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] > 1)

result = tx.sortBy(lambda x: (x[0][0], x[0][1]))
output = result.map(lambda x: x[0][0] + ',' + x[0][1])
output.saveAsTextFile("task3b.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3b.out
hfs -rm -R task3b.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3b.py task1a.out
hfs -getmerge task3b.out task3b.out
'''
