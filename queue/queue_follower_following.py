__author__ = 'sunary'

from twitter.twitter_services import TwitterService
import os
from utils import my_helper
import pandas as pd
import time
from queue import Queue
from queue import ConsumerQueue


class QueueFollowerFollowing():
    def __init__(self):
        self.queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'
        ), queue_name='FollowersFollowingCrawlByTwitter_PotentialPublishers')

    def read_data(self):
        pd_file = pd.read_excel('/home/diepdt/crawl_data/151102_DACH_seed_publishers/151102_DACH_seed_publishers.xls', encoding='utf-8')
        pd_file_screenname = pd_file['screen_name']

        self.list_screenname = []
        for screen_name in pd_file_screenname:
            self.list_screenname.append(screen_name)

        convert_screen_name = False
        if convert_screen_name:
            twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
            self.list_screenname = twitter_service.get_screen_names_by_ids(self.list_screenname)

    def find_twitter(self):
        self.list_screenname = []
        list_keyword = ['investor', ' ir ']

        pd_file = pd.read_csv('/home/sunary/data_report/result/follower_following_sorted_checkdup.csv')
        for i in range(len(pd_file['0id'])):
            if not my_helper.pandas_null(pd_file['3description'][i]):
                if any(keyword.lower() in str(pd_file['1name'][i]).lower() for keyword in list_keyword) or\
                        any(keyword.lower() in str(pd_file['3description'][i]).lower() for keyword in list_keyword):
                    self.list_screenname.append(pd_file['2screen_name'][i])

        print len(self.list_screenname)

        # fo = open('/home/sunary/data_report/result/new_seed.csv', 'a')
        # for screenname in self.list_screenname:
        #     fo.write(screenname + '\n')
        # fo.close()

    def push_to_queue(self):
        for screenname in self.list_screenname:
            try:
                print screenname
                self.queue.post(screenname.strip())
                time.sleep(1)
            except Exception as e:
                print 'Exception: %s' % e

    def remove_from_queue(self):
        self.queue = ConsumerQueue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='FollowersFollowingCrawlByTwitter_PotentialPublishers'
        ), queue_name='FollowersFollowingCrawlByTwitter_PotentialPublishers')

        def my_callback(body, message):
            print body
            # message.ack()

        self.queue.on_message = my_callback
        self.queue.run()
        pass


if __name__ == '__main__':
    queue_follower_following = QueueFollowerFollowing()
    queue_follower_following.read_data()
    # queue_follower_following.find_twitter()
    queue_follower_following.push_to_queue()
    # queue_follower_following.remove_from_queue()