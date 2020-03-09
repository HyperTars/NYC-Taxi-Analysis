import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task4c-sql").getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c20.cast('string').alias('name'),
                 df._c5.cast('DECIMAL(10, 2)').alias('fare'))

result = data.groupBy('name').agg(f.sum('fare')) \
    .select('name', f.col('sum(fare)').alias('revenue')) \
    .sort(f.col('revenue').desc()).limit(10) \
    .write.csv('task4c-sql.out', quoteAll=False, header=False,
               quote='', ignoreTrailingWhiteSpace=False)

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task4c-sql.out
hfs -rm -R task4c-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task4c-sql.py task1b-sql.out
hfs -getmerge task4c-sql.out task4c-sql.out
hfs -rm -R task4c-sql.out
cat task4c-sql.out
'''
