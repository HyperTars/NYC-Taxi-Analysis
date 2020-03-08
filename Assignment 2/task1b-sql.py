import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql import functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("task1b-sql").getOrCreate()

fares = spark.read.format('csv') \
        .options(header='true', inferschema='false') \
        .load(sys.argv[1]).na.fill('')
licenses = spark.read.format('csv') \
        .options(header='true', inferschema='false') \
        .load(sys.argv[2]).na.fill('')

result = fares.join(licenses, 'medallion', 'inner') \
        .orderBy("medallion", "hack_license", "pickup_datetime")

res = result.withColumn("name", F.when(col('name').contains(','),
                        F.concat(F.lit('"'), col('name'), F.lit('"')))
                        .otherwise(col('name')))


res.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
                         res.medallion, res.hack_license, res.vendor_id,
                         res.pickup_datetime, res.payment_type,
                         res.fare_amount, res.surcharge, res.mta_tax,
                         res.tip_amount, res.tolls_amount, res.total_amount,
                         res.name, res.type, res.current_status,
                         res.DMV_license_plate, res.vehicle_VIN_number,
                         res.vehicle_type, res.model_year,
                         res.medallion_type, res.agent_number, res.agent_name,
                         res.agent_telephone_number, res.agent_website,
                         res.agent_address, res.last_updated_date,
                         res.last_updated_time)) \
                        .write.save('task1b-sql.out', format="text")
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

# Attributes

'''
