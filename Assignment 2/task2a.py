import sys
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

#lines = sc.textFile("task1aSamp.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(','))
fa = lines.map(lambda x: float(x[15]))
fa1 = fa.filter(lambda x: x >= 0 and x <= 5).count()
fa2 = fa.filter(lambda x: x > 5 and x <= 15).count()
fa3 = fa.filter(lambda x: x > 15 and x <= 30).count()
fa4 = fa.filter(lambda x: x > 30 and x <= 50).count()
fa5 = fa.filter(lambda x: x > 50 and x <= 100).count()
fa6 = fa.filter(lambda x: x > 100).count()
result = sc.parallelize([("0,5", fa1), \
        ("5,15", fa2), \
        ("15,30", fa3), \
        ("30,50", fa4), \
        ("50,100", fa5), \
        (">100", fa6)])
output = result.map(lambda x: x[0] + ',' + str(x[1]))
output.saveAsTextFile("task2a.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2a.out
hfs -rm -R task2a.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2a.py task1a.out
hfs -getmerge task2a.out task2a.out
'''
