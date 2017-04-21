__author__ = 'sunary'


import datetime
from utils.my_mongo import Mongodb
from queue import ConsumerQueue
from queue import Queue
from bson.objectid import ObjectId

# worker:123cayngaycaydem@rabbitmq-prod2.ssh.sentifi.com
# sentifi:onTheStage@node-staging-01.ireland.sentifi.internal
# worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal

class RunQueue():
    def __init__(self):
        pass

    def get_id_twitter_need_profile(self):
        queue = ConsumerQueue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishers'
        ), queue_name='DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishers')

        mongo_save = Mongodb(db='potential_id_twitter_need_200t', col='user')

        def my_callback(body, message):
            if mongo_save.count({'id': body}) <= 0:
                mongo_save.insert({'id': body})
            message.ack()

        queue.on_message = my_callback
        queue.run()

    def get_queue_size(self):
        queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishers'
        ), queue_name='DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishers')
        print str(datetime.datetime.now()) + ' <> ' + str(queue.get_size())

    def move_message(self):
        queue1 = ConsumerQueue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='TagTweetWorkerProcessor_TweetTaggingTmp3'
        ), queue_name='TagTweetWorkerProcessor_TweetTaggingTmp3')

        queue2 = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='TagTweetOrange_TweetTaggingTmp3'
        ), queue_name='TagTweetOrange_TweetTaggingTmp3')

        def my_callback1(body, message):
            try:
                queue2.post(body)
                message.ack()
            except Exception, e:
                print e

        queue1.on_message = my_callback1
        queue1.run()

    def post_queue(self):
        queue = Queue(dict(
            Uri='worker:123cayngaycaydem@rabbitmq-prod2.ssh.sentifi.com',
            Name='TwitterProfileWorker_Publisher'
        ), queue_name='TwitterProfileWorker_Publisher')

        pd_file  = open('tw_id.csv', 'r')
        list_id = pd_file.read()
        list_id = list_id.split('\n')

        for id in list_id:
            try:
                queue.post(str(id))
            except Exception, e:
                print e

    def tagging_messages(self):
        queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='TagTweetOrange_TweetTaggingTmp3'
        ), queue_name='TagTweetOrange_TweetTaggingTmp3')

        tweet = Mongodb(host= 'mongo7.ireland.sentifi.internal', db= 'sentifi', col= 'tweet_potential_publisher')

        from_object =  '559265c00000000000000000'
        to_object =  '559365c00000000000000000'
        max_object = '559365c00000000000000000'

        res = tweet.find(
            {"_id": {"$gt": ObjectId(from_object), "$lt": ObjectId(to_object)}}, ['_id'])
        for doc in res:
            try:
                queue.post(str(doc['_id']))
            except:
                pass

    def post_staging(self):
        # tweet = Mongodb(host= 'mongo6.ssh.sentifi.com', db= 'sentifi', col= 'tweet')
        tweet = Mongodb(host= 'mongo6.ssh.sentifi.com', db= 'moreover', col= 'articles')
        from_object =  '55a7e3100000000000000000'

        queue = Queue(dict(
            Uri='sentifi:onTheStage@node-staging-01.ireland.sentifi.internal',
            # Name='TagTweetWorkerProcessor_StagingTweetTagging3'
            Name='TagNewsWorkerProcessor_StagingNewsTagging2'
        ))

        res = tweet.find({"_id": {"$gt": ObjectId(from_object)}}, ['_id'])

        for doc in res:
            try:
                queue.post(str(doc['_id']))
            except:
                pass

    def debug_queue(self):
        queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name='DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishersExchange'))

        queue.post('234324')
        queue.close()


if __name__ == '__main__':
    run_queue = RunQueue()
    run_queue.debug_queue()
