# -*- coding: utf-8 -*-
__author__ = 'sunary'


import tweepy
from tweepy.streaming import StreamListener, json
from tweepy import Stream
from twitter.twitter_key import TwitterKey
import os


class Listener(StreamListener):
    '''
    override StreamListener
    '''
    def __init__(self, user_id):
        super(Listener, self).__init__()

        self.user_id = user_id

    def on_data(self, data):
        '''
        Get tweet and retweet only
        '''
        data = json.loads(data)
        if data.get('text'):
            print data
        return True

    def on_error(self, status):
        print status

class TwitterStream():

    user_id = '111489227'

    def __init__(self, twitter_key_path):
        self.twitter_key = TwitterKey(twitter_key_path)
        self.get_engine()

    def get_engine(self):
        key = self.twitter_key.get()
        listener = Listener(self.user_id)
        auth = tweepy.OAuthHandler(key['consumer_key'], key['consumer_secret'])
        auth.set_access_token(key['access_token'], key['access_token_secret'])
        self.streamer = Stream(auth, listener)

    def filter(self):
        '''
        filter tweet
        '''
        self.streamer.filter(follow=[self.user_id])

    def user_stream(self):
        '''
        user stream
        '''
        self.streamer.userstream()

if __name__ == '__main__':
    twitter_stream = TwitterStream('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
    twitter_stream.filter()
    # twitter_stream.user_stream()
