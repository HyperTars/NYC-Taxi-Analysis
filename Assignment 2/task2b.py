import sys
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# file = sc.textFile("task1aSamp.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
passenger = lines.map(lambda x: (x[7], 1))
distPass = passenger.reduceByKey(lambda x, y: x+y)
result = distPass.sortBy(lambda x: x[0])
output = result.map(lambda x: x[0] + ',' + str(x[1]))
output.saveAsTextFile("task2b.out")

sc.stop()

'''
module load python/gnu/3.4.4
module load spark/2.2.0
rm -rf task2b.out
hfs -rm -R task2b.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2b.py task1a.out
hfs -getmerge task2b.out task2b.out
'''
