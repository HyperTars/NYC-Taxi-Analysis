import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task3c-sql").getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c0.cast('string').alias('medallion'),
                 df._c10.cast('float').alias('plon'),
                 df._c11.cast('float').alias('plat'),
                 df._c12.cast('float').alias('dlon'),
                 df._c13.cast('float').alias('dlat'))

taxi = data.groupBy('medallion').count() \
    .select('medallion', f.col('count').alias('total'))

zero = data.filter(data.plon == 0).filter(data.plat == 0) \
    .filter(data.dlon == 0).filter(data.dlat == 0) \
    .groupBy('medallion').count() \
    .select('medallion', f.col('count').alias('zero'))

result = taxi.join(zero, 'medallion', 'left')
res = result.withColumn('percentage', 100 * result['zero'] / result['total'])
output = res.select(res.medallion, res.percentage.cast('DECIMAL(10, 2)')) \
    .orderBy('medallion').na.fill(0.00) \
    .write.csv('task3c-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3c-sql.out
hfs -rm -R task3c-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3c-sql.py task1a-sql.out
hfs -getmerge task3c-sql.out task3c-sql.out
hfs -rm -R task3c-sql.out
wc -l task3c-sql.out
head -n 20 task3c-sql.out
'''
