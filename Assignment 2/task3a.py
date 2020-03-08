import sys
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
fa = lines.map(lambda x: float(x[15])).filter(lambda x: x < 0).count()
output = sc.parallelize([fa])
output.saveAsTextFile("task3a.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3a.out
hfs -rm -R task3a.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3a.py task1a.out
hfs -getmerge task3a.out task3a.out
'''
