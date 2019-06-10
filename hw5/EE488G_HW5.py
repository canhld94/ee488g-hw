#######################################################################################################################
"""                                  EE488G Database and Bigdata system  HW5                                        """
#######################################################################################################################
"""
   * pyspark rdd methods : https://spark.apache.org/docs/1.1.1/api/python/pyspark.rdd.RDD-class.html
"""
import sys
from pyspark.sql import SparkSession


def warmup_sql (spark, datafile):
   
   df = spark.read.parquet(datafile)
   df.createOrReplaceTempView("flights")
   rq = spark.sql("SELECT depdelay FROM flights LIMIT 100")
   
   rq.show()
   
   return rq

def warmup_rdd (spark, datafile):
   
   d = spark.read.parquet(datafile).rdd
   
   r = d.map(lambda x: x['depdelay'])   
   r = spark.sparkContext.parallelize(r.take(100)) # limit 100
   
   return r

def Q1 (spark, datafile):
   # Select all fights that leave from \Seattle, WA", and return the destination city
   # names. Only return each city name once.
   # Result Size: 79 rows (50 rows on the small dataset), 1 mins on Dataproc
   df = spark.read.parquet(datafile)

   # your code here
   # create a logical view of the table
   df.createOrReplaceTempView("flights")
   # run sql query
   rq = spark.sql("SELECT DISTINCT destcityname from flights WHERE origincityname = 'Seattle, WA'")

   return rq 

def Q2 (spark, datafile):
   # Implement the same query as above, but use the RDD API.
   # Result Size: 79 rows (50 rows on the small dataset), 21 mins on Dataproc
   d = spark.read.parquet(datafile).rdd
    
   # your code here
   # filter the dataset by the origin city name
   r = d.filter(lambda x: x['origincityname'] == 'Seattle, WA')
   # select the destination city name with
   r = r.map(lambda x: x['destcityname'])
   # remove dupplicated tupples
   r = r.distinct()

   return r
   
def Q3 (spark, datafile):
   # Find the number of non-cancelled fights per month that departs from each city, re-
   # turn the results in a RDD where the key is a pair (i.e., a Tuple2 object), consisting
   # of a String for the departing city name, and an Integer for the month.
   # Result Size: 4383 rows (281 rows on the small dataset), 25 mins on Dataproc
   d = spark.read.parquet(datafile).rdd
    
   # your code here
   # filter the data set by the cancelled value
   r = d.filter(lambda x: x['cancelled'] == 0)
   # select the city and the month
   r = r.map(lambda x: (x['origincityname'], x['month']))
   # create key-value pair
   r = r.map(lambda x: (x, 1))
   # reduce by key
   r = r.reduceByKey(lambda x, y: x+y)
   # reoder the output by city name
   r = r.sortByKey(1)
   
   return r

def Q4 (spark, datafile):
   # Find the name of the city that is connected to the most number of other cities within
   # a single hop fight. Return the result as a pair that consists of a String for the city
   # name, and an Integer for the total number of cities connected within a single hop.
   # Result Size: 1 row, 24 mins on Dataproc 
   d = spark.read.parquet(datafile).rdd
   
   # your code here
   # selec the origin city and destination city
   r = d.map(lambda x: (x['origincityname'], x['destcityname']))
   # remove the dunplicated tuple
   r = r.distinct()
   # create key value pair
   r = r.map(lambda (k, value): (k, 1))
   # reduce by key
   r = r.reduceByKey(lambda x, y: x+y)
   # reduce the dataset by greater function
   r = r.reduce(lambda (k0, value0), (k1, value1): (k0, value0) if value0 >= value1 else (k1, value1))
   
   # debug
   print r
   # convert tuple to rdd
   rd = spark.sparkContext.parallelize([r])

   return rd

def Q5 (spark, datafile):
   # Compute the average delay from all departing fights for each city. Flights with null
   # delay values (due to cancellation or otherwise) should not be counted. Return the
   # results in a RDD where the key is a String for the city name, and the value is a
   # double for the average delay in minutes.
   # Result Size: 383 rows (281 rows on the small dataset), 25 mins on Dat-aproc
   d = spark.read.parquet(datafile).rdd
   
   # your code here
   # filter the dataset by the delay values
   r = d.filter(lambda x: x['depdelay'] != None)
   # create key-value
   r = r.map(lambda x: (x['origincityname'], x['depdelay']))
   # group by key
   r = r.groupByKey()
   # calculate the avg of the value in key-value pair
   r = r.map(lambda (k, value): (k, float(sum(value))/len(value)))
   
   return r


# gcloud dataproc jobs submit pyspark --cluster cluster-eb3d ./EE488G_HW5.py -- gs://ee488g-data/flights_full gs://dataproc-0d6a5ffa-2d11-40d5-9fbf-d523daf05883-asia-northeast1/run
if __name__ == '__main__':
    
   input_data = sys.argv[1]
   output = sys.argv[2]
   
   spark = SparkSession.builder.appName("HW5").getOrCreate()

   # """ - WarmUp SQL - """
   # rq = warmup_sql(spark, input_data)
   # rq.rdd.repartition(1).saveAsTextFile(output)
   
   # """ - WarmUp RDD - """
   # r = warmup_rdd(spark, input_data)
   # r.repartition(1).saveAsTextFile(output)
   
   # # """ - Problem 1 - """
   # rq1 = Q1(spark, input_data)
   # rq1.rdd.repartition(1).saveAsTextFile(output)
   
   # """ - Problem 2 - """
   # r2 = Q2(spark, input_data)
   # r2.repartition(1).saveAsTextFile(output)
   
   # """ - Problem 3 - """
   # r3 = Q3(spark, input_data)
   # r3.repartition(1).saveAsTextFile(output)
   
   """ - Problem 4 - """
   r4 = Q4(spark, input_data)
   r4.saveAsTextFile(output)
   
   # """ - Problem 5 - """
   # r5 = Q5(spark, input_data)
   # r5.repartition(1).saveAsTextFile(output)

   spark.stop()
