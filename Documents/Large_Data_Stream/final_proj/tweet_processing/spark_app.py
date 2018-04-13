from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import traceback
from nltk.corpus import stopwords
import time

i = 0

stop_words = [word for word in stopwords.words('english')] + [word for word in stopwords.words('french')] + [word for word in stopwords.words('spanish')]
stop_words = [word.lower() for word in stopwords.words('english')] + ['', '-', 'la', 'de', 'que', 'en', 'like', 'get', 'el', 'one', "I'm", "Hi", "Lo", '.', "We're", "great", "anyone", "see", "es", "much", "can't", "eu", "las", "da", "ya", "con", "gonna", "q", "un", "someone", "u", "thing", "se", "always", "go", "around", "going", "got", "could", "really", 'para', "e", "take", "e", "also", "last", "know", "think", "want", "need", "Thank", "today", "would", "everything", "everyone", "every", "make", "Thanks", "ever", "even", "many", "might"]
stop_words += [word.upper() for word in stop_words]
stop_words += [word[0:1].upper() + word[1:].lower() for word in stop_words]
stop_words = set(stop_words)

VALID_LETTERS = set(c for c in "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM1234567890',./#-_")

def is_valid(word):
    return all(c in VALID_LETTERS for c in word)

window_size = 10

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# creat the Streaming Context from the above spark context with window size 5 seconds
ssc = StreamingContext(sc, window_size)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9001)

def normalize(word):
    if len(word) >= 2:
        if word[0] == "#":
            word = word[1:]
        if word[-1] == ".":
            word = word[:-1]
    return word

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']



def print_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        print(rdd.collect())
    except:
        print(traceback.print_exc())

def process_topk(rdd, k=10):
    print("----------- %s -----------" % str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ))
    global i
    num = i % 10
    i += 1
    try:
        topk = rdd.top(k, lambda x:x[1])
        highest = topk[0][1]
        print(topk)
        with open("output_{}.json".format(num), "w") as f:
            f.write('[\n')
            for element in topk:
                f.write("{"+'"text":"{}", "size":{}'.format(element[0], float(element[1])/highest*100)+"},\n")
            f.write(']\n')
    except:
        print(traceback.print_exc())


def release_rdd(time, rdd):
    rdd.unpersist()

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

words = words.map(lambda word: normalize(word))

words = words.filter(lambda word: is_valid(word) and word not in stop_words)

# # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.map(lambda x: (x, 1))

# # adding the count of each hashtag to its last count
tags_totals = hashtags.reduceByKey(lambda x, y: x+y)


# tags_totals.pprint(10)
# print(tags_totals.collect())
tags_totals.foreachRDD(lambda rdd: process_topk(rdd, k=20))
# dataStream.foreachRDD(release_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

