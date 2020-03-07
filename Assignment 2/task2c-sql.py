import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("task2c-sql").getOrCreate()
sc = SparkContext.getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load("task1a-sql.out").na.fill('')

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

fees = df.select(df._c3.cast('DATE').alias('date'),
                 df._c15.cast('DECIMAL(10,2)').alias('fare'),
                 df._c16.cast('DECIMAL(10,2)').alias('surcharge'),
                 df._c18.cast('DECIMAL(10,2)').alias('tip'),
                 df._c19.cast('DECIMAL(10,2)').alias('tolls'))

result = fees.withColumn('revenue', expr("fare + surcharge + tip")) \
    .select('date', 'revenue', 'tolls').groupBy('date') \
    .sum('revenue', 'tolls').orderBy('date') \
    .write.csv('task2c-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2c-sql.out
hfs -rm -R task2c-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2c-sql.py task1a-sql.out
hfs -getmerge task2c-sql.out task2c-sql.out
hfs -rm -R task2c-sql.out
cat task2c-sql.out
'''
