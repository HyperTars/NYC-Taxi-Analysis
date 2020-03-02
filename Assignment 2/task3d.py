import sys
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# file = sc.textFile("task1a.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
taxi = lines.map(lambda x: (x[0], x[1])).distinct()
taxi = taxi.map(lambda x: ((x[1]), 1))
driver = taxi.reduceByKey(lambda x, y: x + y)

result = driver.sortBy(lambda x: x[0])
output = result.map(lambda x: x[0] + ',' + str(x[1]))
output.saveAsTextFile("task3d.out")

sc.stop()

'''
module load python/gnu/3.4.4
module load spark/2.2.0
rm -rf task3d.out
hfs -rm -R task3d.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3d.py task1a.out
hfs -getmerge task3d.out task3d.out
'''
