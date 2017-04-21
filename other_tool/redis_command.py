__author__ = 'sunary'


from queue.my_queue import Queue
from twitter.twitter_services import TwitterService
from redis.client import StrictRedis
from rediscluster import StrictRedisCluster
import pandas as pd
import os


class RedisCommand():

    def __init__(self):
        '''
        Usages:
            redis-cli -h host:port
        '''

    def test_redis(self):
        redis = StrictRedis(host='localhost', port=6379)
        redis.delete('taolao')
        taolao = [1, 2, 3, 4, 5]
        redis.lpush('taolao', *taolao)
        redis.expire('taolao', 3600)
        print redis.lrange('taolao', 0, -1)

    def test_redis_cluster(self):
        redis_cluster = StrictRedisCluster(startup_nodes = [{'host': '10.1.2.130', 'port': 6379}])
        redis_cluster.delete('taolao')
        taolao = [1, 2, 3, 4, 5]
        redis_cluster.lpush('taolao', *taolao)
        redis_cluster.expire('taolao', 3600)
        print redis_cluster.lrange('taolao', 0, -1)

    def push_following(self):
        '''
        crawl following - push to redis cluster
                        - post user_id to queue
        '''
        redis_cluster = StrictRedisCluster(startup_nodes = [{'host': '10.1.2.130', 'port': 6379}])
        pd_file = pd.read_csv('/home/nhat/data/40 subcribers Twitter ID.csv')

        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        queue = Queue({
            'uri':['amqp://sentifi:onTheStage@node-staging-01.ireland.sentifi.internal'],
            'name':'Community_Calculate_Leaderboard'
        })

        for user_id in pd_file['Twitter ID']:
            list_following = twitter_service.get_following(user_id=user_id)

            if list_following:
                for i in range(len(list_following)):
                    list_following[i] = 'tw-' + str(list_following[i])

                redis_cluster.delete('sns:tw-%s:following' % (user_id))
                redis_cluster.lpush('sns:tw-%s:following' % (user_id), *list_following)
                redis_cluster.expire('sns:tw-%s:following' % (user_id), 7776000)


            json_id = {
                'args': ['tw', str(user_id)],
                'task': 'sentifi.community.tasks.leaderboard.calculate_leaderboard',
                'id': 'tw-' + str(user_id)
            }
            queue.post(json_id)
            print user_id

if __name__ == '__main__':
    redis_command = RedisCommand()
    redis_command.push_following()
