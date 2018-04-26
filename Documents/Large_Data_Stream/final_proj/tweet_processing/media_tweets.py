from config import consumer_key, consumer_secret, access_token, access_token_secret
from tweepy import OAuthHandler
import tweepy
import json

name = ["BBCWorld", "CNN", "ABC", "NBCNews", "FoxNews", "WIRED", "YahooNews", "WSJbusiness", "usnews", "ReutersPolitics"]
file = ["BBCWorld.json", "CNN.json‏", "ABC.json", "NBCNews.json", "FoxNews.json", "WIRED.json", "YahooNews.json", "WSJbusiness.json", "usnews.json", "ReutersPolitics.json"]

if __name__ == '__main__':

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

    # This is a part that can get at most 3200 recent tweets of a user. But I don't know how to put them into json files
    #  https://stackoverflow.com/questions/42225364/getting-whole-user-timeline-of-a-twitter-user
    #  http://docs.tweepy.org/en/v3.5.0/cursor_tutorial.html
    #     for tweet in tweepy.Cursor(api.user_timeline, screen_name='@realDonaldTrump',tweet_mode="extended").items():
    #         print (tweet._json['full_text'])

    # Store recent 200 tweets for a user."full_text" is the tweets content.
    for i in range(10):
        status = api.user_timeline(screen_name = name[i], count = 200, tweet_mode ="extended")
        json.dumps(status)
        with open(file[i], "w") as f:
            json.dump(status, f)
