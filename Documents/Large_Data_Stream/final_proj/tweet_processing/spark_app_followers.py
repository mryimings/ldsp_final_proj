from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import traceback
from nltk.corpus import stopwords
import time
import json
from operator import add
from nltk.tokenize import word_tokenize

i = 0

stop_words = [word for word in stopwords.words('english')] + [word for word in stopwords.words('french')] + [word for word in stopwords.words('spanish')]
stop_words = [word.lower() for word in stop_words] + ['', '-', 'la', 'de', 'que', 'en', 'like', 'get', 'one', "i'm", "we're", "great", "anyone", "see", "es", "much", "can't", "eu", "las", "da", "ya", "con", "gonna", "q", "un", "someone", "u", "thing", "se", "always", "go", "around", "going", "got", "could", "really", 'para', "e", "take", "e", "also", "last", "know", "think", "want", "need", "thank", "today", "would", "everything", "everyone", "every", "make", "Thanks", "ever", "even", "many", "might", "getting", "los", "..", "said", "say", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "us", "del", "una", "never", "time", "web", "things", "since", "still", "ass", "new"]
stop_words += [word.upper() for word in stop_words]
stop_words += [word[0:1].upper() + word[1:].lower() for word in stop_words]
stop_words = set(stop_words)

VALID_LETTERS = set(c for c in "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM1234567890',./#-_")



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
dataStream = ssc.socketTextStream("localhost", 9009)

def is_valid(word):
    return len(word) >= 3 and all(c in VALID_LETTERS for c in word) and any(c.isalpha() for c in word)

def normalize(word):
    if len(word) >= 2:
        if word[0] == "#":
            word = word[1:]
        if word[-1] == ".":
            word = word[:-1]
    return word.lower()

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
        json_list = []

        for element in topk:
            json_list.append({"text": str(element[0]), "size": float(element[1])/float(highest)*100})

        with open("./output_data/output_{}.json".format(num), "w") as f:
            json.dump(json_list, f)

    except:
        print(traceback.print_exc())

# For the keys, split by ' ' after split by "!@#$[]"
def process(record):
    for key in record[0].split(' '):
        yield key, record[1]



words = dataStream.map(lambda x: tuple(x.split("!@#$[]")))

words = words.filter(lambda x: len(x)==2)
words = words.map(lambda x: (word_tokenize(x[0]), x[1]))
words = words.map(lambda x: ([normalize(word) for word in x[0] if (is_valid(word) and word not in stop_words)], x[1]))

#words = words.map(lambda word: normalize(word[0]))
#words = words.filter(lambda word: is_valid(word[0]) and word[0] not in stop_words)


words.foreachRDD(lambda x : print(x.collect()))

#
# words = dataStream.flatMap(lambda line: line.split(" "))
# #
# words = words.map(lambda word: normalize(word))
# #
# words = words.filter(lambda word: is_valid(word) and word not in stop_words)
# #
# hashtags = words.map(lambda x: (x, 1))
# #
# tags_totals = hashtags.reduceByKey(lambda x, y: x+y)
# #
# words.foreachRDD(lambda rdd: process_topk(rdd, k=20))

ssc.start()

ssc.awaitTermination()

