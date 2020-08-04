import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('task2d-sql').getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c0.cast('string').alias('medallion'),
                 df._c3.cast('DATE').alias('date'))

result = data.groupBy('medallion').agg(f.count('*'), f.countDistinct('date')) \
    .select('medallion', f.col('count(1)').alias('trips'),
            f.col('count(DISTINCT date)').alias('days'))

res = result.withColumn('average', result['trips']/result['days'])

output = res.select(res.medallion, res.trips, res.days,
                    res.average.cast('DECIMAL(10, 2)')) \
    .orderBy('medallion') \
    .write.csv('task2d-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task2d-sql.out
hfs -rm -R task2d-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task2d-sql.py task1a-sql.out
hfs -getmerge task2d-sql.out task2d-sql.out
hfs -rm -R task2d-sql.out
wc -l task2d-sql.out
head task2d-sql.out
tail task2d-sql.out
'''
