import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task4b-sql").getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c18.cast('string').alias('type'),
                 df._c5.cast('DECIMAL(10, 2)').alias('fare'),
                 df._c8.cast('DECIMAL(10, 2)').alias('tip'))

result = data.withColumn('percentage', data['tip']/data['fare'])
res = result.groupBy('type') \
    .agg(f.count('*'), f.sum('fare'), f.sum('percentage')) \
    .select('type', f.col('count(1)').alias('trips'),
            f.col('sum(fare)').alias('revenue'),
            f.col('sum(percentage)').alias('percentage'))
output = res.withColumn('avg', 100 * res['percentage'] / res['trips']) \
    .select('type', 'trips', 'revenue',
            f.col('avg').cast('DECIMAL(10, 2)').alias('avg')) \
    .orderBy('type') \
    .write.csv('task4b-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task4b-sql.out
hfs -rm -R task4b-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task4b-sql.py task1b-sql.out
hfs -getmerge task4b-sql.out task4b-sql.out
hfs -rm -R task4b-sql.out
cat task4b-sql.out
'''
