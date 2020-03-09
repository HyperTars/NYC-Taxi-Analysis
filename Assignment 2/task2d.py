import sys
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))

data_trips = lines.map(lambda x: ((x[0]), 1))
trips = data_trips.reduceByKey(lambda x, y: x + y)

data_days = lines.map(lambda x: ((x[0], x[3][:10]), 1))
days = data_days.reduceByKey(lambda x, y: x + y)
days = days.map(lambda x: (x[0][0], 1))
days = days.reduceByKey(lambda x, y: x + y)

result = trips.join(days) \
    .map(lambda x: (x[0], x[1][0], x[1][1], float((x[1][0] / x[1][1]))))
result = result.sortBy(lambda x: x[0])

output = result.map(lambda x: x[0] + ',' + str(x[1]) + ',' + str(x[2])
                    + ',' + '%.2f' % x[3])

output.saveAsTextFile("task2d.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2d.out
hfs -rm -R task2d.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2d.py task1a.out
hfs -getmerge task2d.out task2d.out
hfs -rm -R task2d.out
cat task2d.out
'''
