'''
module load python/gnu/3.4.4
module load spark/2.2.0

pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate*()
'''

# spark
lines = file.mapPartitions(lambda x: reader(x)). \
    filter(lambda line: len(line) > 1 and 'medallion' not in line)
map = lines.map(lambda x: ((x[0], x[1]), x[2]))

file = sc.textFile("xx.csv")
fileSplit = file.flatMap(lambda x: x.split(','))
map = fileSplit.map(lambda x: (x, 1))
map.collect()   # 得到上一次变换的结果
map.take(3)     # 取前3个
wc = map.reduceByKey(lambda a, b: a + b).collect()
wc.sortByKey().collect()
wc.saveAsTextFile("wc")

map.join(wc).collect()
# 两个都有 key, value 构成

from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)   # .reduceByKey(lambda x, y: x + y)
    counts.saveAsTextFile("wc.out")
    sc.stop()
'''
spark-submit wordcount.py sherlock.txt
hfs -getmerge wc.out wc.out
head -n 20 wc.out
'''
############################################################

errors.cache()
text_file = spark.textfile("...")
errors = text_file.filter(lambda line: "ERROR" in line)
errors.count()
errors.filter(lambda line: "MySQL" in line).count()
errors.filter(lambda line: "MySQL" in line).collect()

# spark sql
from pyspark.sql import SparkSession
#这个spark初始化有问题
spark = SparkSession.builder.appName("t1")
# parking可用
parking = spark.read.format('csv').options(header='true', inferschema='true').load('xx.csv')
parking.show()

# 这个spark初始化可以用
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


sailors = spark.read.json("/user/ecc290/sailors.json")
reserves = spark.read.json("/user/ecc290/reserves.json")
# print dataframe
sailors.show()
reserves.show()

sailors.printSchema()
sailors.select("sname").show()
sailors.select(sailors['sname'], sailors['age'] + 1).show()
sailors.filter(sailors['age'] > 21).show()
sailors.groupBy("age").count().show()

sailors.createOrReplaceTempView("sailors")
reserves.createOrReplaceTempView("reserves")
spark.sql("SELECT * FROM sailors").show()

from pyspark.sql import Row
sc = spark.sparkContext
# Load a text file and convert each line to a Row.
lines = sc.textFile("/user/ecc290/boats.txt")
parts = lines.map(lambda l: l.split(","))
boatsRDD = parts.map(lambda p: Row(bid=int(p[0]), name=p[1], color=p[2]))

#Infer the schema, and register the DataFrame as a table.
boats = spark.createDataFrame(boatsRDD)
boats.show()
# Register the DataFrame as a SQL temporary view
boats.createOrReplaceTempView("boats")
spark.sql("SELECT * FROM boats").show()

boats = spark.read.json("/user/ecc290/boats.json"

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT sname FROM sailors \
     WHERE age >= 13 AND age <= 19")

boats.filter("color like '%red%'").show()
spark.sql("select * from boats \
    where color like '%red%'").show()

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.sname).collect() 
for name in teenNames:
    print(name)

sailors.select("*").write.save("sailorscsv.csv", format="csv")
hfs -getmerge sailorscsv.csv sailorscsv.csv

spark.sql("SELECT sname, age FROM sailors").show()
spark.sql("SELECT S.sname, S.age FROM sailors S").show()


####
file1 = sc.textFile(sys.argv[1], 1)
file2 = sc.textFile(sys.argv[2], 1)

lines1 = file1.mapPartitions(lambda x: reader(x)). \
    filter(lambda line: len(line) > 1)
lines2 = file2.mapPartitions(lambda x: reader(x)). \
    filter(lambda line: len(line) > 1)

if 'fare_amount' in lines1:
    fares = lines1
    trips = lines2
elif 'fare_amount' in lines2:
    fares = lines2
    trips = lines1

map_fares = fares.map(lambda x: ((x[0], x[1], x[2], x[3]), \
    (x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
map_trips = trips.map(lambda x: ((x[0], x[1], x[2], x[5]), \
    (x[3], x[4], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13])))
result = map_trips.join(map_fares)

result.sortBy(lambda x: (x[0][0], x[0][1], x[0][3]), False, 1)
result.collect()



import sys
from csv import reader
from pyspark import SparkContext
sc= SparkContext.getOrCreate()

file_fares = sc.textFile("fares_samp.csv")
file_trips = sc.textFile("trips_samp.csv")

file_trips = sc.textFile(sys.argv[1], 1)
file_fares = sc.textFile(sys.argv[2], 1)

line_trips = file_trips.mapPartitions(lambda x: reader(x)).filter(lambda line: len(line) > 1 and 'medallion' not in line)
line_fares = file_fares.mapPartitions(lambda x: reader(x)).filter(lambda line: len(line) > 1 and 'medallion' not in line)

map_trips = line_trips.map(lambda x: (x[0] + ',' + x[1] + ',' + x[2] + ',' + x[5], \
    x[3] + ',' + x[4] + ',' + x[6] + ',' + x[7] + ',' + x[8] + ',' + x[9] + ',' + \
    x[10] + ',' + x[11] + ',' + x[12] + ',' + x[13]))
map_fares = line_fares.map(lambda x: (x[0] + ',' + x[1] + ',' + x[2] + ',' + x[3], \
    x[4] + ',' + x[6] + ',' + x[7] + ',' + x[8] + ',' + x[9] + ',' + x[10]))

map_fares = line_fares.map(lambda x: ((x[0], x[1], x[2], x[3]), \
    (x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
map_trips = line_trips.map(lambda x: ((x[0], x[1], x[2], x[5]), \
    (x[3], x[4], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13])))
result = map_trips.join(map_fares)


result = map_trips.join(map_fares, 1)
result = result.map(lambda t: (t[0]+','+t[1][0]+','+t[1][1]))
result.collect()

result = result.map(lambda x: (x[0], x[1], x[3]))
result.sortBy(lambda x: (x[0], x[1], x[3])).collect()
result = sorted(result, key=lambda x: x[0][0], reverse=True)
output.saveAsTextFile("task1a.out")
