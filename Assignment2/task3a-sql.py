import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("task3a-sql").getOrCreate()
sc = SparkContext.getOrCreate()

df = spark.read.format('csv').options(header='false', inferschema='false') \
    .load(sys.argv[1]).na.fill('')

fa = df.select(df._c15.cast('float').alias('val'))
fa_invald = fa.filter(fa.val < 0).count()

result = spark.read.json(sc.parallelize([{"count": fa_invald}])).na.fill('') \
    .write.csv('task3a-sql.out', quoteAll=False, header=False, quote='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task3a-sql.out
hfs -rm -R task3a-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task3a-sql.py task1a-sql.out
hfs -getmerge task3a-sql.out task3a-sql.out
hfs -rm -R task3a-sql.out
cat task3a-sql.out
'''
