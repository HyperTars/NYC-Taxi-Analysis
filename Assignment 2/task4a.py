'''
Fares join licenses
medallion, hack_license, vendor_id, pickup_datetime, payment_type, 
fare_amount, surcharge, mta_tax, tip_amount, tolls_amount,
total_amount
name, type, current_status, DMV_license_plate, vehicle_VIN_number,
vehicle_type, model_year, medallion_type, agent_number, agent_name,
agent_telephone, agent_website, agent_adderss, last_updated, last_updated_time

total_revenue = sigma(fare_amount)
avg_tip = 100 * avg(fare / tip)
'''

import sys
from pyspark import SparkContext
from itertools import accumulate


def calsum(values):
    return [[k[0], k[1]] for k in zip(accumulate([i[0] for i in values]), \
            accumulate([i[1] for i in values]))]


sc = SparkContext.getOrCreate()
# file = sc.textFile("task1b.out")
file = sc.textFile(sys.argv[1], 1)

lines = file.map(lambda line: line.split(',MEDALLION,CUR,')). \
        map(lambda x: ((x[0].split(',')), (x[1].split(','))))
trips = lines.map(lambda x: (x[1][2], (float(x[0][5]), float(x[0][8]))))

result = trips.groupByKey().mapValues(calsum) \
        .map(lambda x: (x[0], x[1][-1][0], x[1][-1][1] / x[1][-1][0] * 100)) \
        .map(lambda x: (x[0], '%.2f' % x[1], '%.2f' % x[2]))
output = result.sortBy(lambda x: x[0]). \
        map(lambda x: x[0] + ',' + x[1] + ',' + x[2])
output.saveAsTextFile("task4a.out")

sc.stop()

'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf task4a.out
hfs -rm -R task4a.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task4a.py task1b.out
hfs -getmerge task4a.out task4a.out
'''
