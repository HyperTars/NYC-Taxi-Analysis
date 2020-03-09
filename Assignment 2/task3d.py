import sys
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
taxi = lines.map(lambda x: (x[0], x[1])).distinct().map(lambda x: ((x[1]), 1))
result = taxi.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
output = result.map(lambda x: x[0] + ',' + str(x[1]))
output.saveAsTextFile("task3d.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3d.out
hfs -rm -R task3d.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3d.py task1a.out
hfs -getmerge task3d.out task3d.out
hfs -rm -R task3d.out
wc -l task3d.out
head -n 20 task3d.out
'''
