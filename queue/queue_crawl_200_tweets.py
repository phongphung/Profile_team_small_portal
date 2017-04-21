__author__ = 'sunary'


from sentifi_queue.queue import Queue
from utils import my_connection, my_helper


class QueueCrawl200Tweets():
    '''
    Just run it!

    Get user_id from user_tracking has status = 4,
    Then post to queue TweetCrawlByTwitterIdProcessor_PotentialPublishers to crawl 200 tweets for each user_id
    After crawl 200 tweets, the status of user_id was change to 0
    '''

    def __init__(self):
        self.onetime_push = 1000000
        
        crawl_exchange = 'OrangeCrawler_InputExchange'

        self.queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal',
            Name=crawl_exchange
        ), queue_name=crawl_exchange)
        self.logger = my_helper.init_logger(self.__class__.__name__)
        self.user_ids = []
        self.con_da0 = None

    def get_data(self):
        self.con_da0 = my_connection.da0_connection()
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT user_id
            FROM user_tracking
            WHERE status = 4
            LIMIT %s
        ''', [self.onetime_push])
        self.user_ids = cur_da0.fetchall()
        self.logger.info('Done: get %s data' % len(self.user_ids))

    def push_to_queue(self):
        for user_id, in self.user_ids:
            try:
                self.queue.post(str(user_id))
            except Exception, e:
                print e
        self.logger.info('Done: post to queue')

    def get_priority_data(self):
        with my_connection.da0_connection() as pg_conn:
            with pg_conn.cursor() as pg_cur:
                pg_cur.execute("""SELECT u.user_id
FROM twitter.tw_user u
INNER JOIN twitter.follower AS f ON u.user_id = f.follower_id
LEFT JOIN twitter.user_tracking AS ut ON u.user_id = ut.user_id
WHERE f.user_id IN (82923521,31696279,24868529,24870448,282475987,600864377,362928602,19901161,5560422,148684833,20592690,5776022,19667190,18770504,19762728,19237729,49608548,49607121,19709133,160499861,784790730,3147264177,213222038,2412676796,1625765664,41331822,86959871,23335539,111256652,67672642,10387152,260771406,385323163,14582043,17875880,11060982,2438715588,934822172,64366543,77009778,459092531,14425402,312010780,72519897,15040978,17999077,21881428,534421876,414026849,464712850,24249367,29219633,2461432297,375616589,1874916482)
AND ut.status = 4

UNION

SELECT u.user_id
FROM twitter.tw_user u
INNER JOIN twitter.follower AS f ON u.user_id = f.user_id
LEFT JOIN twitter.user_tracking AS ut ON u.user_id = ut.user_id
WHERE f.follower_id IN (82923521,31696279,24868529,24870448,282475987,600864377,362928602,19901161,5560422,148684833,20592690,5776022,19667190,18770504,19762728,19237729,49608548,49607121,19709133,160499861,784790730,3147264177,213222038,2412676796,1625765664,41331822,86959871,23335539,111256652,67672642,10387152,260771406,385323163,14582043,17875880,11060982,2438715588,934822172,64366543,77009778,459092531,14425402,312010780,72519897,15040978,17999077,21881428,534421876,414026849,464712850,24249367,29219633,2461432297,375616589,1874916482)
AND ut.status = 4""")
                self.user_ids = pg_cur.fetchall()
                self.logger.info('Done: get %s data' % len(self.user_ids))


if __name__ == '__main__':
    queue_crawl_200_tweets = QueueCrawl200Tweets()
    queue_crawl_200_tweets.get_priority_data()
    # queue_crawl_200_tweets.push_to_queue()
