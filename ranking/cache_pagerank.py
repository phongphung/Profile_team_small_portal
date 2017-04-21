__author__ = 'sunary'


from rediscluster import StrictRedisCluster
from redis import StrictRedis
import pickle
import sys


class CachePagerank():
    REDIS_TEMP_TTL = 2 * 7 * 24 * 3600
    def __init__(self):
        # self.redis_cluster = StrictRedisCluster(startup_nodes = [{'host': '10.1.2.130', 'port': 6379}])
        self.redis_cluster = StrictRedis(host='ranking.ssh.sentifi.com', port=6379)

    def read_pkl(self, pagerank_pkl):
        fo = open(pagerank_pkl, 'rb')
        data = pickle.load(fo)
        data = data[['vertice', 'score']]
        fo.close()
        self.pagerank_user = data[data['vertice'].str.contains('u')]

    def cache_signal(self, week, year):
        num_users = len(self.pagerank_user)
        processed_users = 0
        while True:
            processed_users += 1
            next_row = next(self.pagerank_user.iterrows())[1]
            if processed_users >= num_users:
                return

            user_id = next_row['vertice'][1:]
            self.redis_cluster.hmset('sns:%s-%s:signal:%s-%s' % ('tw', user_id, year, week), {8: next_row['score']})
            self.redis_cluster.expire('sns:%s-%s:signal:%s-%s' % ('tw', user_id, year, week), self.REDIS_TEMP_TTL)

if __name__ == '__main__':
    cache_pagerank = CachePagerank()
    try:
        # year, week = int(sys.argv[1]), int(sys.argv[2])
        year, week = 2015, 15
        cache_pagerank.read_pkl('/home/nhat/data/%s_%s_pagerank.pkl' % (week, year))
        cache_pagerank.cache_signal(week, year)
    except:
        print 'Format: cache_pagerank.py year week'