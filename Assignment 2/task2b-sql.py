import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("task2b-sql").getOrCreate()
sc = SparkContext.getOrCreate()

#df = spark.read.format('csv').options(header='false', inferschema='false') \
#    .load("task1a-sql.out").na.fill('')

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

passenger = df.select(df._c7.cast('int').alias('passenger'))
result = passenger.groupBy('passenger').count() \
    .select('passenger', f.col('count').alias('count')) \
    .orderBy('passenger') \
    .write.csv('task2b-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2b-sql.out
hfs -rm -R task2b-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2b-sql.py task1a-sql.out
hfs -getmerge task2b-sql.out task2b-sql.out
hfs -rm -R task2b-sql.out
cat task2b-sql.out
'''
