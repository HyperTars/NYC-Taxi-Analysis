import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("task2a-sql").getOrCreate()
sc = SparkContext.getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

fa = df.select(df._c15.cast('float').alias('val'))

fa1 = fa.filter(fa.val >= 0).filter(fa.val <= 5).count()
fa2 = fa.filter(fa.val > 5).filter(fa.val <= 15).count()
fa3 = fa.filter(fa.val > 15).filter(fa.val <= 30).count()
fa4 = fa.filter(fa.val > 30).filter(fa.val <= 50).count()
fa5 = fa.filter(fa.val > 50).filter(fa.val <= 100).count()
fa6 = fa.filter(fa.val > 100).count()

result = [{"range": "0,5", "val": fa1},
          {"range": "5,15", "val": fa2},
          {"range": "15,30", "val": fa3},
          {"range": "30,50", "val": fa4},
          {"range": "50,100", "val": fa5},
          {"range": ">100", "val": fa6}]

res = spark.read.json(sc.parallelize(result)).na.fill('') \
    .write.csv('task2a-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2a-sql.out
hfs -rm -R task2a-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2a-sql.py task1a-sql.out
hfs -getmerge task2a-sql.out task2a-sql.out
hfs -rm -R task2a-sql.out
cat task2a-sql.out
'''
