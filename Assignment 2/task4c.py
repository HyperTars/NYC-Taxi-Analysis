import sys
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
# file = sc.textFile("task1b.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(',MEDALLION,CUR,')). \
        map(lambda x: ((x[0].split(',')), (x[1].split(','))))
trips = lines.map(lambda x: (x[1][6], float(x[0][5])))

result = trips.groupByKey().mapValues(sum).sortBy(lambda x: x[1], False). \
        map(lambda x: x[0] + ',' + '%.2f' % x[1]).take(10)
output = sc.parallelize(list(result))
output.saveAsTextFile("task4c.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task4c.out
hfs -rm -R task4c.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task4c.py task1b.out
hfs -getmerge task4c.out task4c.out
'''
