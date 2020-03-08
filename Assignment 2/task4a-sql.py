import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("task4a-sql").getOrCreate()
sc = SparkContext.getOrCreate()

# df = spark.read.format('csv').options(header='false', inferschema='false') \
#    .load('task1b-sql.out').na.fill('')

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c16.cast('string').alias('vehicle'),
                 df._c5.cast('DECIMAL(10, 2)').alias('fare'),
                 df._c8.cast('DECIMAL(10, 2)').alias('tip'))

result = data.withColumn('percentage', data['tip']/data['fare'])
res = result.groupBy('vehicle') \
    .agg(f.count('*'), f.sum('fare'), f.sum('percentage')) \
    .select('vehicle', f.col('count(1)').alias('trips'),
            f.col('sum(fare)').alias('revenue'),
            f.col('sum(percentage)').alias('percentage'))
output = res.withColumn('avg', 100 * res['percentage'] / res['trips']) \
    .select('vehicle', 'trips', 'revenue',
            f.col('avg').cast('DECIMAL(10, 2)').alias('avg')) \
    .orderBy('vehicle') \
    .write.csv('task4a-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task4a-sql.out
hfs -rm -R task4a-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task4a-sql.py task1b-sql.out
hfs -getmerge task4a-sql.out task4a-sql.out
hfs -rm -R task4a-sql.out
cat task4a-sql.out
'''
