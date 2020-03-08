import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("task2d-sql").getOrCreate()
sc = SparkContext.getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c0.cast('string').alias('medallion'),
                 df._c3.cast('DATE').alias('date'))

trips = data.groupBy('medallion').count() \
    .select('medallion', f.col("count").alias("total_trips"))
days = data.groupBy('medallion').agg(f.countDistinct('date')) \
    .select('medallion', f.col("count(DISTINCT date)").alias("days_driven"))

result = trips.join(days, 'medallion', 'inner')
res = result.withColumn('average', result['total_trips']/result['days_driven'])

output = res.select(res.medallion, res.total_trips, res.days_driven,
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
cat task2d-sql.out
'''
