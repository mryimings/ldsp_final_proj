import time
from config import consumer_key, consumer_secret, access_token, access_token_secret
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import traceback
from nltk.corpus import stopwords

TCP_IP = "localhost"
TCP_PORT = 9001
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


# keyword = [w for w in stopwords.words("english")]
#keyword = ["NBA", "NFL", "MLB", "NHL", "basketball", "baseball", "football", "sports", "Toronto", "Boston", "Philadephia", "New York", "Brooklyn", "Washington", "Atlanta", "Orlando", "Miami", "Charlotte", "Cleveland", "Chicago", "Detroit", "Indiana", "Milwaukee", "Memphis", "New Orland", "Houston", "Dallas", "San Antonio", "Los Angeles", "Utah", "Golden State", "Denver", "Seattle", "Oklahoma", "Minnesota", "Portland", "Sacramento"]
keyword = ["classical music", "classical music", "Baroque music", "contemporary music", "Rock and Roll", "Rap", "R&B", "Jazz", "Pop", "New Age music", "melody", "rhythm", "tempo", "album","singer", "band"]
#keyword = ["clothes", "Hollister", "A&F", "American Eagle", "Aeropostale", "GAP", "The North Face", "POLO", "Calvin Klein", "UGG", "Forever21", "Levi's", "H&M", "ZARA", "Topshop", "CK", "Everlane", "shoes", "boots", "suit", "sweater", "trousers", "tie", "coat", "dress", "jacket", "blouse", "shirt", "skirt", "jeans", "hat"]
#keyword = ["machine learning","Artificial Intelligence", "Knowledge Representation","NLP", "Reinforcement Learning", "Data Mining", "Artificial Neural Network","Soft Computing", "Artificial Life", "Artificial Neural Network"]
#keyword = ["game", "gaming", "Nintendo", "EA", "Blizzard", "Ubisoft", "SCE", "KONAMI", "CAPCOM", "SQUARE ENIX", "BANDAI NAMCO", "VIVENDI", "Steam", "Nintendo Switch", "Play Station", "Xbox", "Origin", "GOG", "Uplay", "NS", "PS"]
#keyword = ["book", "movie", "Science fiction", "Drama", "Action and Adventure", "Romance", "Mystery", "Horror", "Self help", "Guide", "Children's", "fiction", "non-fiction", "Poetry", "Comics", "Dictionaries", "Encyclopedias", "Art", "Cookbooks", "Diaries", "Journals", "Series", "Fantasy", "Biographies", "anthologies"]
#keyword = ["car", "Ford", "Toyota", "Chevrolet", "Honda", "Nissan", "Jeep", "Hyundai", "Subaru", "Kia", "GMC", "Ram", "Dodge", "Mercedes-Benz", "Volkswagen", "BMW", "Lexus", "Mazda", "Audi", "Buick", "Chrysler"]
#keyword = ["food", "restaurant", "Calories", "cookie", "chicken", "cheese", "hot dog", "burger", "fast food", "Appetizers", "Breads‎", "Chocolate", "Convenience foods", "Dessert", "Dumplings", "Egg", "Meat‎", "Noodles‎", "Pancake", "Pasta", "Pie", "salad", "Pudding", "Sandwiche", "Seafood‎", "snack", "Soup", "stew", "Sugar‎", "Vegetable"]

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            if 'text' in full_tweet:
                tweet_text = full_tweet['text']
                tcp_connection.send((tweet_text + '\n').encode())
            else:
                continue
        except:
            print(traceback.print_exc())


# This is a basic listener that just prints received tweets to stdout.
class Listener(StreamListener):
    def __init__(self):
        self.whatever = 0
        self.f = open("tweets_data.txt", "a")

    def on_data(self, data):
        try:
            self.f.write(data)
            self.f.write("\n")
            conn.send((data+"\n").encode())
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(1)

        return True

    def on_error(self, status):
        print(status)
        return True

    def close_filestream(self):
        self.f.close()


if __name__ == '__main__':

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    listener = Listener()
    twitter_stream = Stream(auth, listener)
    print(type(twitter_stream))
    start_time = time.time()
    while True:
        try:
            twitter_stream.filter(track=keyword)
        except KeyboardInterrupt:
            listener.close_filestream()

    # 获取类似于内容句柄的东西
    # api = tweepy.API(auth)
    #
    # # 打印其他用户主页上的时间轴里的内容
    # public_tweets = api.user_timeline('realDonaldTrump')
    #
    # for tweet in public_tweets:
    #     print(tweet.text)



