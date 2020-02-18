import os
import string
import csv
filePath = './samples/fares_samp.csv'
#filePath = os.environ["mapreduce_map_input_file"]
tripFlag = False
trips = 'trips'
#if trips in os.getenv('mapreduce_map_input_file'):
if trips in filePath:
    tripFlag = True

csvfile = open(filePath, 'r')

#print(csvfile)
csvreader = csv.DictReader(csvfile)
count = 0
totalList = []
for row in csvreader:
    print len(row)
    #key_dict = {'medallion': row['medallion'], 'hack_license': row['hack_license'], 'vendor_id': row['vendor_id'], 'pickup_datetime': row['pickup_datetime']}
    #value_dict = {'t': 0}
    #temp_dict = {'key_dict': key_dict, 'value_dict': value_dict}
    key = ''
    value = ''
    key = str(row['medallion']) + ',' + \
        str(row['hack_license']) + ',' + \
        str(row['vendor_id']) + ',' + \
        str(row['pickup_datetime'])
    
    if tripFlag:
        value = str(row['rate_code']) + ',' + \
            str(row['store_and_fwd_flag']) + ',' + \
            str(row['dropoff_datetime']) + ',' + \
            str(row['passenger_count']) + ',' + \
            str(row['trip_time_in_secs']) + ',' + \
            str(row['trip_distance']) + ',' + \
            str(row['pickup_longitude']) + ',' + \
            str(row['pickup_latitude']) + ',' + \
            str(row['dropoff_longitude']) + ',' + \
            str(row['dropoff_latitude'])
    else:
        value = str(row['payment_type']) + ',' + \
            str(row['fare_amount']) + ',' + \
            str(row['surcharge']) + ',' + \
            str(row['mta_tax']) + ',' + \
            str(row['tip_amount']) + ',' + \
            str(row['tolls_amount']) + ',' + \
            str(row['total_amount'])
    tempList = []
    tempList.append(key)
    tempList.append(value)
    totalList.append(tempList)
    #medallion.append(row['medallion'])
    #hack_license.append(row['hack_license'])
    #vendor_id.append(row['vendor_id'])
    #pickup_datetime.append(row['pickup_datetime'])
    count += 1
    #print(key)
    #print(value)
    
print len(totalList)
for row in totalList:
    if row:
        print '%s\t%s' % (row[0], row[1])

#    line = line.strip(',')
#    words = line.split()
#    for word in words:
#        w = word.strip(string.punctuation)
#        if w:
#            print '%s\t%s' % (w.lower(), 1)


