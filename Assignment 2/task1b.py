import sys
from csv import reader
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# file_fares = sc.textFile("fares_samp.csv")
# file_licenses = sc.textFile("licenses_samp.csv")

file_fares = sc.textFile(sys.argv[1], 1)
file_licenses = sc.textFile(sys.argv[2], 1)

line_fares = file_fares.mapPartitions(lambda x: reader(x)). \
    filter(lambda line: len(line) > 1 and 'medallion' not in line)
line_licenses = file_licenses.mapPartitions(lambda x: reader(x)). \
    filter(lambda line: len(line) > 1 and 'medallion' not in line)


map_fares = line_fares.map(lambda x: (x[0], \
    (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
map_licenses = line_licenses.map(lambda x: (x[0], \
    (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], \
    x[11], x[12], x[13], x[14], x[15])))

result = map_fares.join(map_licenses)
result = result.sortBy(lambda x: (x[0], x[1][0][1], x[1][0][3]))

output = result.map(lambda x: x[0] + ',' + \
    ','.join(x[1][0]) + ',' + ','.join(x[1][1]))
output.saveAsTextFile("task1b.out")

'''
module load python/gnu/3.4.4
module load spark/2.2.0
rm -rf task1b.out
hfs -rm -R task1b.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
task1b.py /user/hc2660/hw2data/Fares.csv \
/user/hc2660/hw2data/Licenses.csv
hfs -get task1b.out
hfs -getmerge task1b.out task1b.txt
hfs -put task1b.txt
'''
