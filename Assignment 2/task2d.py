import sys
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
# file = sc.textFile("task1a.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))

data_trips = lines.map(lambda x: ((x[0]), 1))
trips = data_trips.countByKey()
data_days = lines.map(lambda x: (x[0], x[3][:10]))
days = data_days.distinct().countByKey()

dic = {}
for key in trips.keys():
    val = []
    val.append(trips[key])
    val.append(days[key])
    if (days[key] != 0):
        val.append('%.2f' % (trips[key] / days[key]))
    else:
        val.append('%.2f' % 0)
    dic[key] = val

output = sc.parallelize(list(dic.items()))
output = output.map(lambda x: x[0] + ',' + str(x[1][0]) + ',' + str(x[1][1]) \
        + ',' + str(x[1][2]))

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
'''
