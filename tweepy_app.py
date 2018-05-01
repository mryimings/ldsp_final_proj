import time
from config import consumer_key, consumer_secret, access_token, access_token_secret
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import traceback
import sys
from nltk.corpus import stopwords

TCP_IP = "localhost"
TCP_PORT = int(sys.argv[1])
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


keyword_dict = {"default": [w for w in stopwords.words("english")],
                "sports": ["NBA", "NFL", "MLB", "NHL", "basketball", "baseball", "football", "sports", "Toronto", "Boston", "Philadephia", "Washington", "Atlanta", "Orlando", "Miami", "Charlotte", "Cleveland", "Chicago", "Detroit", "Indiana", "Milwaukee", "Memphis", "New Orland", "Houston", "Dallas", "San Antonio", "Los Angeles", "Utah", "Golden State", "Denver", "Seattle", "Oklahoma", "Minnesota", "Portland", "Sacramento", "rockets", "warriors", "spurs", "blazers", "jazz", "thunders", "timberwolves", "raptors", "cavs", "cavalires", "celtics", "76ers", "heat", "bucks", "wizards", "pacers", "lakers", "mavericks", "clippers", "harden", "durant", "curry", "paul", "kobe", "lebron", "playoffs"],
                "politics": ["bbc", "cnn", "fox", "trump", "hillary", "obama", "democrat", "republican", "doc", "gop", "lgbt", "tax", "immigrant", "korea", "@BernieSanders", "@chuckschumer", "@SenatorDurbin", "@NancyPelosi", "@WhipHoyer", "@jacobinmag", "@TheDemocrats", "@AmerLiberal", '@FoxNews', '@amconmag', '@BreitbartNews', '@VP', '@GOP', '@Conservatives', '@CR'],
                "music": ["classical music", "classical music", "Baroque music", "contemporary music", "Rock and Roll", "Rap", "R&B", "Jazz", "Pop", "New Age music", "melody", "rhythm", "tempo", "album","singer", "band"],
                "dressing": ["clothes", "Hollister", "A&F", "American Eagle", "Aeropostale", "GAP", "The North Face", "POLO", "Calvin Klein", "UGG", "Forever21", "Levi's", "H&M", "ZARA", "Topshop", "CK", "Everlane", "shoes", "boots", "suit", "sweater", "trousers", "tie", "coat", "dress", "jacket", "blouse", "shirt", "skirt", "jeans", "hat"],
                "technology": ["machine learning","artificial intelligence", "knowledge representation","nlp", "reinforcement learning", "data mining", "artificial neural network","cloud computing", "artificial life", "artificial neural network", "python", "java", "c++", "golang", "javascript", "frontend", "backend", "operating system", 'computer network', "alphago", 'google', 'facebook', 'amazon', 'linkedin', 'uber', 'airbnb', 'yelp', "tripadvisor", "apple inc", "microsoft", "qualcomm", "intel", "yahoo"],
                "gaming": ["game", "gaming", "Nintendo", "EA", "Blizzard", "Ubisoft", "SCE", "KONAMI", "CAPCOM", "SQUARE ENIX", "BANDAI NAMCO", "VIVENDI", "Steam", "Nintendo Switch", "Play Station", "Xbox", "Origin", "GOG", "Uplay", "NS", "PS"],
                "cars": ["ford", "toyota", "chevrolet", "honda", "nissan", "jeep", "hyundai", "subaru", "kia", "gmc", "ram", "dodge", "mercedes-benz", "volkswagen", "bmw", "lexus", "mazda", "audi", "buick", "chrysler", "cadillac", "porsche", "ferrari"],
                "food": ["food", "restaurant", "calories", "cookie", "chicken", "cheese", "hot dog", "burger", "fast food", "appetizers", "breads‎", "chocolate", "convenience foods", "dessert", "dumpling", "egg", "meat‎", "noodles‎", "pancake", "pasta", "pie", "salad", "pudding", "sandwich", "seafood‎", "snack", "soup", "stew", "sugar‎", "vegetable"],
}

# keyword = keyword_dict[sys.argv[2] if len(sys.argv) >= 3 else "default"]
# keyword =  keyword + [word.upper() for word in keyword] + [word[0].upper() + word[1:].lower() for word in keyword]

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

    def on_data(self, data):
        try:
            data = json.loads(data)
            if 'text' in data:
                data = data['text']
                conn.send((data+"\n").encode())
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            time.sleep(1)

        return True

    def on_error(self, status):
        print(status)
        return True



if __name__ == '__main__':

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    listener = Listener()
    twitter_stream = Stream(auth, listener)
    print(type(twitter_stream))
    start_time = time.time()
    while True:
        try:
            with open("./file_pipline/streaming_args.json", "r") as f:
                json_obj = json.load(f)
            keyword = keyword_dict[json_obj["keyword"]]
            keyword = keyword + [word.upper() for word in keyword] + [word[0].upper() + word[1:].lower() for word in keyword]
            twitter_stream.filter(track=keyword)
        except KeyboardInterrupt:
            break



