import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession.builder.appName("task1a-sql").getOrCreate()

# df_trips = spark.read.format('csv').options(header='true', \
#        inferschema='false').load("/user/hc2660/hw2data/Trips.csv") \
#        .na.fill({'store_and_fwd_flag': 'N'}) \
#        .na.fill('')
# df_fares = spark.read.format('csv').options(header='true', \
#        inferschema='false').load("/user/hc2660/hw2data/Fares.csv") \
#       .na.fill('')

df_trips = spark.read.format('csv').options(header='true', inferschema='false') \
        .load(sys.argv[1]) \
        .na.fill({'store_and_fwd_flag': 'N'}) \
        .na.fill('')
df_fares = spark.read.format('csv').options(header='true', inferschema='false') \
        .load(sys.argv[2]) \
        .na.fill('')

result = df_trips.join(df_fares, ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime'], 'inner') \
        .orderBy("medallion", "hack_license", "pickup_datetime")

result.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
        result.medallion, result.hack_license, result.vendor_id,
        result.pickup_datetime, result.rate_code, result.store_and_fwd_flag,
        result.dropoff_datetime, result.passenger_count, result.trip_time_in_secs,
        result.trip_distance, result.pickup_longitude, result.pickup_latitude,
        result.dropoff_longitude, result.dropoff_latitude, result.payment_type,
        result.fare_amount, result.surcharge, result.mta_tax,
        result.tip_amount, result.tolls_amount, result.total_amount)) \
        .write.save('task1a-sql.out', format="text")

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

# Attributes
result.medallion
result.hack_license
result.vendor_id
result.pickup_datetime
result.rate_code
result.store_and_fwd_flag
result.dropoff_datetime
result.passenger_count
result.trip_time_in_secs
result.trip_distance
result.pickup_longitude
result.pickup_latitude
result.dropoff_longitude
result.dropoff_latitude
result.payment_type
result.fare_amount
result.surcharge
result.mta_tax
result.tip_amount
result.tolls_amount
result.total_amount
'''
