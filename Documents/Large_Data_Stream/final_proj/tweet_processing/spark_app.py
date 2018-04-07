from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import traceback
from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# creat the Streaming Context from the above spark context with window size 2 seconds
ssc = StreamingContext(sc, 5)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9001)


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']



def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        print(rdd.collect())
    except:
        print(traceback.print_exc())
    # print(rdd.collect())


def release_rdd(time, rdd):
    rdd.unpersist()

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

words = words.filter(lambda word: word.isalpha() and word not in stop_words)

# # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.map(lambda x: (x, 1))

# # adding the count of each hashtag to its last count
tags_totals = hashtags.reduceByKey(lambda x, y: x+y)


tags_totals.pprint(10)

dataStream.foreachRDD(release_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

