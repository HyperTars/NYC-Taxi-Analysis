import sys
import string
import unicodedata
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Read in and setup. DO NOT CHANGE.
sc = SparkContext()
spark = SparkSession.builder.appName("hw3"). \
    config("spark.some.config.option", "some-value").getOrCreate()

# Load Data
df = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
# df = spark.read.csv(path='/user/hc2660/hw3data/data_cleaning.csv', header=True)
df.createOrReplaceTempView('df')

##################
##### Task1 ######
##################
# Task 1-a

duplicate = df.count() - df.dropDuplicates(['summons_number']).count()
na = df.filter('summons_number is NULL').count()
task1a_result = duplicate + na

##################
# Task 1-b

task1b_result = spark.sql('select plate_type, count(*) from df group by plate_type order by count(1) DESC')
task1b_result = task1b_result.select('plate_type', f.col('count(1)').alias('count'))

##################
# Task 1-c

t1cRes = df.withColumn('plate_type', f.when(df['plate_type'] == '999', 'NULL').otherwise(df['plate_type'])).groupBy('plate_type').count()
task1c_result = t1cRes.sort(f.col('count').desc())
# task1c_result.createOrReplaceTempView('t1c')
# spark.sql('select * from t1c where plate_type = "NULL"').show()

##################
# Task 1-d

task1d_result1 = df.filter('violation_county is NULL').count()

task1d_result2 = df.filter('violation_county is NOT NULL').count()

##################

"""
Output Method - Do Not Change. UNCOMMENT the following lines when you have the tasks finished
"""
sc.parallelize([str(task1a_result)]).saveAsTextFile("hw3-task1-a.out")

task1b_result.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("hw3-task1-b.out")

task1c_result.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("hw3-task1-c.out")

# task1d_result1.coalesce(1).rdd.saveAsTextFile("hw3-task1-d1.out")
sc.parallelize([str(task1d_result1)]).saveAsTextFile("hw3-task1-d1.out")
sc.parallelize([str(task1d_result2)]).saveAsTextFile("hw3-task1-d2.out")

###################
##### Task2 #######
###################

# Convert relevant columns to RDD here
plate_id_rdd = df.rdd.map(lambda x: x[14]).filter(lambda x: x is not None)
street_name_rdd = df.rdd.map(lambda x: x[17]).filter(lambda x: x is not None)

##################
# Task 2-a: Implementing Fingerprint.

def fingerprint(value):
    # remove whitespace around the string and lowercase it
    key = value.strip().lower()

    # remove all punctuation and control characters
    for punct in (set(key) & set(string.punctuation)):
        key = key.replace(punct, '')
    key = key.replace('\t', '')

    # normalize extended western characters to their ASCII representation
    # decode: 'NFKD' 'NFD', re-encode: + .encode('ascii', 'ignore')
    key_norm = unicodedata.normalize('NFKD', key)

    # split the string into whitespace-separated tokens
    key_split = key_norm.split()

    # sort the tokens and remove duplicates
    key_ordered = sorted(set(key_split))

    # join the tokens back together
    key = ' '.join(key_ordered)
    return (key, value)

#################
# Task 2-b: Implementing N-gram Fingerprint

def ngram_fingerprint(value, n=2):
    # change all characters to their lowercase representation
    ngram = value.lower()

    # remove all punctuation, whitespace and control characters
    for punct in (set(ngram) & set(string.punctuation)):
        ngram = ngram.replace(punct, '')
    ngram = ngram.replace('\t', '').replace(' ', '')

    # obtain all the string n-grams
    ngram_split = [ngram[i: i + n] for i in range(len(ngram) - n + 1)]

    # sort the n-grams and remove duplicates
    ngrams = list(set(ngram_split))
    ngrams.sort()

    # join the sorted n-grams back together
    ngram_joined = ''.join(ngrams)

    # normalize extended western characters to their ASCII representation
    # decode: 'NFKD' 'NFD', re-encode: + .encode('ascii', 'ignore')
    result = unicodedata.normalize('NFKD', ngram_joined)
    return (result, value)


##################
# Task 2-c: Apply Fingerprint to the plate_id column. Output all output clusters.

task2c_fp = plate_id_rdd.map(lambda x: fingerprint(x))
task2c_map = task2c_fp.map(lambda x: (x[0], [x[1]]))
task2c_output = task2c_map.reduceByKey(lambda x, y: x + y)
# task2c_output.count()
# task2c_output.take(20)
task2c_cluster = task2c_output.filter(lambda x: len(set(x[1])) > 1)
# task2c_cluster.count()
# task2c_cluster.collect()
task2c_result = task2c_output

###################
# Task 2-d: Apply N-Gram Fingerprint to the plate_id column. Output all output clusters.

task2d_ngram = plate_id_rdd.map(lambda x: ngram_fingerprint(x, 3))
task2d_map = task2d_ngram.map(lambda x: (x[0], [x[1]]))
task2d_output = task2d_map.reduceByKey(lambda x, y: x + y)
# task2d_output.count()
# task2d_output.take(20)
task2d_cluster = task2d_output.filter(lambda x: len(set(x[1])) > 1)
# task2d_cluster.count()
# task2d_cluster.collect()
task2d_result = task2d_output

###################
# Task 2-e: Apply Fingerprint to the street_name column. Output the first 20 clusters.

task2e_fp = street_name_rdd.map(lambda x: fingerprint(x))
task2e_map = task2e_fp.map(lambda x: (x[0], [x[1]]))
task2e_output = task2e_map.reduceByKey(lambda x, y: x + y)
# task2e_output.count()
# task2e_output.take(20)
task2e_cluster = task2e_output.filter(lambda x: len(set(x[1])) > 1)
# task2e_cluster.count()
# task2e_cluster.take(20)
task2e_result = task2e_cluster.zipWithIndex().filter(lambda x: x[1] < 20).keys()

##################
# Task 2-f: Apply N-Gram Fingerprint to the street_name column. Output the first 20 clusters

task2f_fp = street_name_rdd.map(lambda x: ngram_fingerprint(x, 1))
task2f_map = task2f_fp.map(lambda x: (x[0], [x[1]]))
task2f_output = task2f_map.reduceByKey(lambda x, y: x + y)
# task2f_output.count()
# task2f_output.take(20)
task2f_cluster = task2f_output.filter(lambda x: len(set(x[1])) > 1)
# task2f_cluster.count()
# task2f_cluster.take(20)
task2f_result = task2f_cluster.zipWithIndex().filter(lambda x: x[1] < 20).keys()

##################
# Task 2-g: Provide your qualitative response in template2.txt.
##################
# Task 2-h: Design and perform transformations to the street_name column here. Output all clusters.

def transform(value):
    # make everything capitalized
    value = value.upper()

    # filter out punctuation
    value = value.replace('[', '').replace('%', '').replace('@', '') \
        .replace('#', '').replace('!', '').replace('.', '').replace(';', '') \
        .replace(']', '').replace(',', '').replace('"', '').replace(",", '')

    # no leading or trailing whitespace
    value = value.strip()

    # collapse consecutive whitespace
    value = ' '.join(value.split())
    return value


task2h_tr = street_name_rdd.map(lambda x: transform(x))
task2h_fp = task2h_tr.map(lambda x: fingerprint(x))
task2h_map = task2h_fp.map(lambda x: (x[0], [x[1]]))
task2h_output = task2h_map.reduceByKey(lambda x, y: x + y)
# task2h_output.count()
# task2h_output.take(20)
task2h_cluster = task2h_output.filter(lambda x: len(set(x[1])) > 1)
# task2h_cluster.count()
# task2h_cluster.take(20)
task2h_result = task2h_cluster

#################
# Task 2-i: Provide your qualitative response in template2.txt.
#################

"""
Output Methods - Do not Change. UNCOMMENT these lines when you have the tasks finished
"""

task2c_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-c.out")
task2d_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-d.out")
task2e_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-e.out")
task2f_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-f.out")
task2h_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-h.out")


'''
module load python/gnu/3.6.5
module load spark/2.4.0

rm -rf hw3-task1-a.out
rm -rf hw3-task1-b.out
rm -rf hw3-task1-c.out
rm -rf hw3-task1-d1.out
rm -rf hw3-task1-d2.out
rm -rf hw3-task2-c.out
rm -rf hw3-task2-d.out
rm -rf hw3-task2-e.out
rm -rf hw3-task2-f.out
rm -rf hw3-task2-h.out
hfs -rm -R hw3-task1-a.out
hfs -rm -R hw3-task1-b.out
hfs -rm -R hw3-task1-c.out
hfs -rm -R hw3-task1-d1.out
hfs -rm -R hw3-task1-d2.out
hfs -rm -R hw3-task2-c.out
hfs -rm -R hw3-task2-d.out
hfs -rm -R hw3-task2-e.out
hfs -rm -R hw3-task2-f.out
hfs -rm -R hw3-task2-h.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
template1.py /user/hc2660/hw3data/data_cleaning.csv
hfs -getmerge hw3-task1-a.out hw3-task1-a.out
hfs -getmerge hw3-task1-b.out hw3-task1-b.out
hfs -getmerge hw3-task1-c.out hw3-task1-c.out
hfs -getmerge hw3-task1-d1.out hw3-task1-d1.out
hfs -getmerge hw3-task1-d2.out hw3-task1-d2.out
hfs -getmerge hw3-task2-c.out hw3-task2-c.out
hfs -getmerge hw3-task2-d.out hw3-task2-d.out
hfs -getmerge hw3-task2-e.out hw3-task2-e.out
hfs -getmerge hw3-task2-f.out hw3-task2-f.out
hfs -getmerge hw3-task2-h.out hw3-task2-h.out
'''
