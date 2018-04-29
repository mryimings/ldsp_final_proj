from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import traceback
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import time
import json

i = 0

stop_words = [word for word in stopwords.words('english')] + [word for word in stopwords.words('french')] + [word for word in stopwords.words('spanish')]
stop_words = [word.lower() for word in stop_words] + ['', '-', 'la', 'de', 'que', 'en', 'like', 'get', 'one', "i'm", "we're", "great", "anyone", "see", "es", "much", "can't", "eu", "las", "da", "ya", "con", "gonna", "q", "un", "someone", "u", "thing", "se", "always", "go", "around", "going", "got", "could", "really", 'para', "e", "take", "e", "also", "last", "know", "think", "want", "need", "thank", "today", "would", "everything", "everyone", "every", "make", "Thanks", "ever", "even", "many", "might", "getting", "los", "..", "said", "say", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "us", "del", "una", "never", "time", "web", "things", "since", "still", "ass", "new", "n't", "http", "https", "amp", "sorry", "take", "took", "taking", "taken", "saying", "said"]
stop_words += [word.upper() for word in stop_words]
stop_words += [word[0:1].upper() + word[1:].lower() for word in stop_words]
stop_words = set(stop_words)

VALID_LETTERS = set(c for c in "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM1234567890',./#-_")



window_size = 5
PORT = int(sys.argv[1])

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
dataStream = ssc.socketTextStream("localhost", PORT)

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

def process_topk(rdd):
    print("----------- %s -----------" % str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ))
    global i
    num = i % 10
    i += 1
    try:
        with open("./file_pipline/streaming_args.json", "r") as f:
            json_obj = json.load(f)
            k = json_obj['MAX_words']
        topk = rdd.top(k, lambda x:x[1])
        total = sum(x[1] for x in topk)
        json_list = []

        for element in topk:
            json_list.append({"text": str(element[0]), "size": float(element[1])/total*400})

        print(json_list)

        with open("./data/realtime_data/slot_{}.json".format(num), "w") as f:
            json.dump(json_list, f)

    except:
        print(traceback.print_exc())



# split each tweet into words
words = dataStream.flatMap(lambda line: word_tokenize(line))


# parsed = dataStream.map(lambda v: json.loads(v))
#
# parsed.pprint(5)
#words = parsed.map(lambda line: line.split(" "))
#
words = words.map(lambda word: normalize(word))

words = words.filter(lambda word: is_valid(word) and word not in stop_words)

# # filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.map(lambda x: (x, 1))

# # adding the count of each hashtag to its last count
tags_totals = hashtags.reduceByKey(lambda x, y: x+y)


# tags_totals.pprint(10)
# print(tags_totals.collect())
tags_totals.foreachRDD(lambda rdd: process_topk(rdd))
# dataStream.foreachRDD(release_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

