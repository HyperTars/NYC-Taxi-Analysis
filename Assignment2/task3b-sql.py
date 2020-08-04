import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task3b-sql").getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c0.cast('string').alias('medallion'),
                 df._c3.cast('string').alias('datetime'))

result = data.groupBy('medallion', 'datetime').count() \
    .select('medallion', 'datetime', f.col("count").alias("cnt"))

res = result.filter(result.cnt > 1).orderBy('medallion', 'datetime') \
    .select('medallion', 'datetime') \
    .write.csv('task3b-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3b-sql.out
hfs -rm -R task3b-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3b-sql.py task1a-sql.out
hfs -getmerge task3b-sql.out task3b-sql.out
hfs -rm -R task3b-sql.out
wc -l task3b-sql.out
head task3b-sql.out
tail task3b-sql.out
'''
