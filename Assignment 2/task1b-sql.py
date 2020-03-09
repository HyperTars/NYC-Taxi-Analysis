import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task1b-sql").getOrCreate()

fares = spark.read.format('csv') \
    .options(header='true', inferschema='false') \
    .load(sys.argv[1]).na.fill('')
licenses = spark.read.format('csv') \
    .options(header='true', inferschema='false') \
    .load(sys.argv[2]).na.fill('')

result = fares.join(licenses, 'medallion', 'inner') \
    .orderBy("medallion", "hack_license", "pickup_datetime")

result.write.csv('task1b-sql.out', quoteAll=False, header=False,
                 emptyValue='', ignoreTrailingWhiteSpace=False)

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task1b-sql.out
hfs -rm -R task1b-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task1b-sql.py /user/hc2660/hw2data/Fares.csv \
/user/hc2660/hw2data/Licenses.csv
hfs -getmerge task1b-sql.out task1b-sql.out
hfs -rm -R task1b-sql.out
hfs -put task1b-sql.out
'''
