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

stop_words = [word for word in stopwords.words('english')]
stop_words = [word.lower() for word in stop_words] + ['', '-', 'la', 'de', 'que', 'en', 'like', 'get', 'one', "i'm", "we're", "great", "anyone", "see", "es", "much", "can't", "eu", "las", "da", "ya", "con", "gonna", "q", "un", "someone", "u", "thing", "se", "always", "go", "around", "going", "got", "could", "really", 'para', "e", "take", "e", "also", "last", "know", "think", "want", "need", "thank", "today", "would", "everything", "everyone", "every", "make", "Thanks", "ever", "even", "many", "might", "getting", "los", "..", "said", "say", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "us", "del", "una", "never", "time", "web", "things", "since", "still", "ass", "new", "n't", "http", "https", "amp", "sorry", "take", "took", "taking", "taken", "saying", "said", "let", "'re", "men", "man", "good", "better", "makes", "making", "told", "else", "first"]
stop_words += [word.upper() for word in stop_words]
stop_words += [word[0:1].upper() + word[1:].lower() for word in stop_words]
stop_words = set(stop_words)

VALID_LETTERS = set(c for c in "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM1234567890',./#-_")

window_size = 5
PORT = int(sys.argv[1])
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, window_size)
ssc.checkpoint("checkpoint_TwitterApp")
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
    if sys.argv[2] == "realtime":
        path = "./data/realtime_data/slot_{}.json"
        with open("./file_pipline/streaming_args.json", "r") as f:
            cloud_num = json.load(f)['MAX_words']
            num = i % cloud_num
    elif sys.argv[2] == "static":
        path = "./data/static_data/slot_{}.json"
        num = i
    else:
        raise ValueError("Unknown Mode")
    i += 1

    try:
        with open("./file_pipline/streaming_args.json", "r") as f:
            json_obj = json.load(f)
            k = json_obj['MAX_words']
            absolute_size = json_obj["Cloud_Width"] * json_obj["Cloud_Height"]
        topk = rdd.top(k, lambda x:x[1])
        total = sum(x[1] for x in topk)
        json_list = []

        constant = 400 / (500 * 500) * absolute_size

        for element in topk:
            json_list.append({"text": str(element[0]), "size": float(element[1])/total*constant})

        print(json_list)

        with open(path.format(num), "w") as f:
            json.dump(json_list, f)

        with open("./file_pipline/current_point.json", "w") as f:
            json.dump({"current_slot": num}, f)

    except:
        print(traceback.print_exc())


words = dataStream.flatMap(lambda line: word_tokenize(line))
words = words.map(lambda word: normalize(word))
words = words.filter(lambda word: is_valid(word) and word not in stop_words)
hashtags = words.map(lambda x: (x, 1))
tags_totals = hashtags.reduceByKey(lambda x, y: x+y)
tags_totals.foreachRDD(lambda rdd: process_topk(rdd))
ssc.start()
ssc.awaitTermination()

