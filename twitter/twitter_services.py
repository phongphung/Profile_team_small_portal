from time import sleep, time

__author__ = 'sunary'


from twitter.twitter_key import TwitterKey
from utils import my_helper
import tweepy
import os


class TwitterService():

    def __init__(self, twitter_key_path):
        self.twitter_key = TwitterKey(twitter_key_path)
        self.get_engine()
        self.logger = my_helper.init_logger(self.__class__.__name__)

    def get_engine(self):
        key = self.twitter_key.get()
        auth = tweepy.OAuthHandler(key['consumer_key'], key['consumer_secret'])
        auth.set_access_token(key['access_token'], key['access_token_secret'])
        self.engine = tweepy.API(auth)
        self.engine.wait_on_rate_limit = True
        self.engine.wait_on_rate_limit_notify = True

    def get_profile(self, user_id):
        while True:
            try:
                return self.engine.get_user(user_id=user_id)
            except tweepy.TweepError as e:
                self.logger.error('get_profile error: %s' % e)
                if not self.error_handle(e):
                    break

    def get_profiles(self, user_ids):
        result = {}
        start = 0
        threshold = 100
        while start < len(user_ids):
            while True:
                try:
                    temp_list_ids = user_ids[start:start + threshold] if len(user_ids) > (start + threshold) else user_ids[start:]
                    docs = self.engine.lookup_users(user_ids=temp_list_ids)
                    for user in docs:
                        result[str(user._json['id'])] = user._json
                    start += threshold
                    break
                except tweepy.TweepError as e:
                    self.logger.error('get_profiles error: %s' % e)
                    if not self.error_handle(e):
                        break
        return result

    def get_profiles_by_screen_names(self, screen_names):
        result = {}
        start = 0
        threshold = 50
        while start < len(screen_names):
            while True:
                try:
                    temp_list_screen_names = screen_names[start:start + threshold] if len(screen_names) > (start + threshold) else screen_names[start:]
                    docs = self.engine.lookup_users(screen_names=temp_list_screen_names)
                    for user in docs:
                        result[str(user._json['screen_name'].strip().lower())] = user._json
                    start += threshold
                    break
                except tweepy.TweepError as e:
                    self.logger.error('get_profiles_by_screen_names error: %s' % e)
                    if not self.error_handle(e):
                        break
        return result

    def get_200tweets_by_id(self, user_id):
        while True:
            try:
                return self.engine.user_timeline(user_id=user_id, count=200)
            except tweepy.TweepError as e:
                self.logger.error('get_200tweets_by_id error: %s' % e)
                if not self.error_handle(e):
                    break

    def get_tweets_by_id(self, user_id, num_tweet):
        all_tweet = []
        while True:
            try:
                for tweet in tweepy.Cursor(self.engine.user_timeline, id= user_id).items(num_tweet):
                    all_tweet.append(tweet)

                return all_tweet
            except tweepy.TweepError as e:
                self.logger.error('get_tweets_by_id error: %s' % e)
                if not self.error_handle(e):
                    return all_tweet

    def get_ids_by_screen_names(self, list_screen_names):
        list_ids = []
        start = 0
        threshold = 100
        while start < len(list_screen_names):
            while True:
                try:
                    temp_list_screen_names = list_screen_names[start:start + threshold] if len(list_screen_names) > (start + threshold) else list_screen_names[start:]
                    docs = self.engine.lookup_users(screen_names=temp_list_screen_names)
                    for user in docs:
                        list_ids.append(user._json['id'])

                    start += threshold
                    break
                except tweepy.TweepError as e:
                    self.logger.error('get_ids_by_screen_names error: %s' % e)
                    if not self.error_handle(e):
                        break

        return list_ids

    def get_screen_names_by_ids(self, list_ids):
        list_screen_names = []
        start = 0
        threshold = 100
        while start < len(list_ids):
            while True:
                try:
                    temp_list_ids = list_ids[start:start + threshold] if len(list_ids) > (start + threshold) else list_ids[start:]
                    docs = self.engine.lookup_users(user_ids= temp_list_ids)
                    for user in docs:
                        list_screen_names.append(user._json['screen_name'])

                    start += threshold
                    break
                except tweepy.TweepError as e:
                    self.logger.error('get_screen_names_by_ids error: %s' % e)
                    if not self.error_handle(e):
                        break

        return list_screen_names

    def get_ids_screen_names(self, list_ids=None, list_screen_names=None):
        if list_ids:
            new_list_ids = []
            list_screen_names = []
            start = 0
            threshold = 100
            while start < len(list_ids):
                while True:
                    try:
                        temp_list_ids = list_ids[start:start + threshold] if len(list_ids) > (start + threshold) else list_ids[start:]
                        docs = self.engine.lookup_users(user_ids=temp_list_ids)
                        for user in docs:
                            new_list_ids.append(user._json['id'])
                            list_screen_names.append(user._json['screen_name'])

                        start += threshold
                        break
                    except tweepy.TweepError as e:
                        self.logger.error('get_ids_screen_names error: %s' % e)
                        if not self.error_handle(e):
                            break

            for i in range(len(list_ids)):
                if list_ids[i] != new_list_ids[i]:
                     new_list_ids[i:i] = [list_ids[i]]
                     list_screen_names[i:i] = ['']

            return (new_list_ids, list_screen_names)
        else:
            list_ids = []
            new_list_screen_names = []
            start = 0
            threshold = 100
            while start < len(list_screen_names):
                while True:
                    try:
                        temp_list_screen_names = list_screen_names[start:start + threshold] if len(list_screen_names) > (start + threshold) else list_screen_names[start:]
                        docs = self.engine.lookup_users(screen_names= temp_list_screen_names)
                        for user in docs:
                            list_ids.append(user._json['id'])
                            new_list_screen_names.append(user._json['screen_name'])

                        start += threshold
                        break
                    except tweepy.TweepError as e:
                        self.logger.error('get_ids_screen_names error: %s' % e)
                        if not self.error_handle(e):
                            break

            for i in range(len(list_screen_names)):
                if list_screen_names[i].lower() != new_list_screen_names[i].lower():
                     list_ids[i:i] = ['']
                     new_list_screen_names[i:i] = [list_screen_names[i]]

            return (list_ids, new_list_screen_names)

    def get_follower(self, user_id=None, screen_name=None):
        list_followers = []
        while True:
            try:
                if user_id:
                    for page in tweepy.Cursor(self.engine.followers_ids, user_id=user_id).pages():
                        for twitter_id in page:
                            list_followers.append(twitter_id)
                else:
                    for page in tweepy.Cursor(self.engine.followers_ids, screen_name=screen_name).pages():
                        for twitter_id in page:
                            list_followers.append(twitter_id)
                return list_followers
            except tweepy.TweepError as e:
                self.logger.error('get_follower error: %s' % e)
                if not self.error_handle(e):
                    return list_followers

    def get_following(self, user_id=None, screen_name=None):
        list_following = []
        while True:
            try:
                if user_id:
                    for page in tweepy.Cursor(self.engine.friends_ids, user_id=user_id).pages():
                        for twitter_id in page:
                            list_following.append(twitter_id)
                else:
                    for page in tweepy.Cursor(self.engine.friends_ids, screen_name=screen_name).pages():
                        for twitter_id in page:
                            list_following.append(twitter_id)
                return list_following
            except tweepy.TweepError as e:
                self.logger.error('get_following error: %s' % e)
                if not self.error_handle(e):
                    return list_following

    def get_lists_by_id(self, twitter_id):
        list_groups = []
        while True:
            try:
                groups = self.engine.lists_memberships(user_id= twitter_id, parser=tweepy.parsers.JSONParser())
                for grp in groups['lists']:
                    list_groups.append(grp)
                return list_groups
            except tweepy.TweepError as e:
                self.logger.error('get_lists_by_id error: %s' % e)
                if not self.error_handle(e):
                    return list_groups

    def get_ownerships_by_id(self, twitter_id):
        list_ownerships = []
        try:
            ownerships = self.engine.lists_memberships(user_id= twitter_id, parser=tweepy.parsers.JSONParser())
            for user in ownerships['lists']:
                list_ownerships.append(user)
        except Exception as e:
            self.logger.error('get_ownerships_by_id error: %s' % e)
            pass
        return list_ownerships

    def get_subscriptions_by_id(self, twitter_id):
        list_subscriptions = []
        try:
            subscriptions = self.engine.lists_subscriptions(user_id= twitter_id, parser=tweepy.parsers.JSONParser())
            for user in subscriptions['lists']:
                list_subscriptions.append(user)
        except Exception as e:
            self.logger.error('get_subscriptions_by_id error: %s' % e)
            pass
        return list_subscriptions

    def error_handle(self, e):
        '''
        handle error
        '''
        try:
            if e[0][0]['code'] == 88:
                self.logger.info('code 88, Rate limit exceeded')
                self.get_engine()
                return True
            elif e[0][0]['code'] == 401:
                self.logger.info('code 401, Unauthorized')
                if not self.twitter_key.report():
                    return False

                self.get_engine()
                return True
            else:
                self.logger.info('code %s, %s' %(e[0][0]['code'], e[0][0]['message']))
                return False
        except Exception as e:
            self.logger.info(e)
            return False

    def _get_sleep_time(self, engine):
        num_call_remain = engine.get_lastfunction_header('x-rate-limit-remaining')
        sleep_time = None
        max_sleep_time = 15 * 60
        if num_call_remain is None:
            self.logger.error('Cannot get rate limit info')
        elif int(num_call_remain) < 1:
            reset_epoch = engine.get_lastfunction_header('x-rate-limit-reset')
            if not reset_epoch:
                self.logger.info('Reset Epoch is None, sleep for 15 minutes')
                sleep_time = max_sleep_time
            else:
                reset_epoch = int(reset_epoch)

                if reset_epoch <= 0:
                    self.logger.info('Reset Epoch not valid, sleep for 15 minutes')
                    sleep_time = max_sleep_time
                else:
                    current = int(time.time())
                    waiting_time = reset_epoch - current
                    if waiting_time < 0:
                        self.logger.info('Twitter REST rate limit, worker go to sleep for 15 minutes')
                        sleep_time = max_sleep_time
                    else:
                        self.logger.info('Twitter REST rate limit, worker go to sleep for ' + str(waiting_time))
                        sleep_time = waiting_time

        return sleep_time


if __name__ == '__main__':
    twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
    # print twitter_service.get_200tweets_by_id(user_id='111489227')
    from pprint import pprint
    # pprint(twitter_service.get_profiles_by_screen_names(screen_names=['Gruenderszene']))
    pprint(twitter_service.get_following(screen_name='diep12892'))
