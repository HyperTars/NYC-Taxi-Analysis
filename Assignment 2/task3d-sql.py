import sys
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("task3d-sql").getOrCreate()
sc = SparkContext.getOrCreate()

# df = spark.read.format('csv').options(header='false', inferschema='false') \
#   .load('task1a-sql.out').na.fill('')

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

data = df.select(df._c1.cast('string').alias('license'),
                 df._c0.cast('string').alias('medallion'))

license = data.groupBy('license').agg(f.countDistinct('medallion')) \
    .select('license', f.col('count(DISTINCT medallion)').alias('taxi_count'))

result = license.orderBy('license') \
    .write.csv('task3d-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3d-sql.out
hfs -rm -R task3d-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3d-sql.py task1a-sql.out
hfs -getmerge task3d-sql.out task3d-sql.out
hfs -rm -R task3d-sql.out
wc -l task3d-sql.out
head -n 20 task3d-sql.out
'''
