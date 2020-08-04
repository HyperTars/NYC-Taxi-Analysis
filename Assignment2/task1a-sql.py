import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("task1a-sql").getOrCreate()

df_trips = spark.read.format('csv') \
    .options(header='true', inferschema='false') \
    .load(sys.argv[1]) \
    .na.fill({'store_and_fwd_flag': ''}) \
    .na.fill('')
df_fares = spark.read.format('csv') \
    .options(header='true', inferschema='false') \
    .load(sys.argv[2]) \
    .na.fill('')

result = df_trips.join(df_fares,
                       ['medallion', 'hack_license', 'vendor_id',
                        'pickup_datetime'], 'inner') \
    .orderBy("medallion", "hack_license", "pickup_datetime")

result.write.csv('task1a-sql.out', quoteAll=False, header=False, emptyValue='')

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task1a-sql.out
hfs -rm -R task1a-sql.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task1a-sql.py /user/hc2660/hw2data/Trips.csv \
/user/hc2660/hw2data/Fares.csv
hfs -getmerge task1a-sql.out task1a-sql.out
hfs -rm -R task1a-sql.out
hfs -put task1a-sql.out
'''
