from config import consumer_key, consumer_secret, access_token, access_token_secret
from tweepy import OAuthHandler
import tweepy
import json
import time

name = ["BBCWorld", "CNN", "ABC", "NBCNews", "FoxNews", "WIRED", "YahooNews", "WSJbusiness", "usnews", "ReutersPolitics"]
file = ["BBCWorld.json", "CNN.json‏", "ABC.json", "NBCNews.json", "FoxNews.json", "WIRED.json", "YahooNews.json", "WSJbusiness.json", "usnews.json", "ReutersPolitics.json"]


left = ["@BernieSanders", "@chuckschumer", "@SenatorDurbin", "@NancyPelosi", "@WhipHoyer", "@jacobinmag", "@TheDemocrats", "@AmerLiberal"]
right = ['@realDonaldTrump', '@FoxNews', '@amconmag', '@BreitbartNews', '@VP', '@GOP', '@Conservatives', '@CR']


if __name__ == '__main__':

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    for left_name in left:
        print(left_name)
        with open('./data/left/'+left_name+'.txt', 'w') as f:
            for tweet in tweepy.Cursor(api.user_timeline, screen_name=left_name,tweet_mode="extended").items():
                f.write(tweet._json['full_text'])
                f.write('\n')
                time.sleep(0.05)
        print(left_name+" complete!")

    for right_name in right:
        print(right_name)
        with open('./data/right/'+right_name+'.txt', 'w') as f:
            for tweet in tweepy.Cursor(api.user_timeline, screen_name=right_name,tweet_mode="extended").items():
                f.write(tweet._json['full_text'])
                f.write('\n')
                time.sleep(0.05)
        print(right_name, "complete!")



