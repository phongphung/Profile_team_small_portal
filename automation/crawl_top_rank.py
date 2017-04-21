__author__ = 'sunary'


import os
from utils import my_connection
import json
from utils import my_helper
from queue.my_queue import Queue
from io import BytesIO
from twitter.twitter_services import TwitterService


class CrawlTopRank():

    def __init__(self):
        self.con_da0 = my_connection.da0_connection()
        self.twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        self.logger = my_helper.init_logger(self.__class__.__name__)

    def process(self):
        list_id_crawl = ['940935636', '2748417222', '2866940860', '2748517662', '2968256134', '2968276594', '2968301068', '2968227417']
        self.logger.info('Start crawl sentifi_network...')
        member_ids = self.crawl_list(list_id_crawl)
        self.insert_user_source(member_ids, 11)
        self.post_to_crawl_profile_queue(member_ids)

        # post to profile

        # append seed and seed-network to id
        list_id_crawl = ['%s:seed:seed-network' % tw_id for tw_id in list_id_crawl]
        if list_id_crawl:
            self.post_to_follower_following_queue(list_id_crawl)

        # now we have following and follower to do it
        # list_id_following_follower = self.crawl_following_follower(list_id_crawl)
        # if list_id_following_follower:
        #         self.post_to_queue(list_id_following_follower)
        #         self.insert_user_source(list_id_following_follower, 11)

        self.logger.info('End crawl sentifi_network...')

        self.logger.info('Start crawl seed_network_by_sentifi_score...')
        list_id_crawl = self.get_top_rank()
        member_ids = self.crawl_list(list_id_crawl)
        self.insert_user_source(member_ids, 10)
        self.post_to_crawl_profile_queue(member_ids)

        # append seed and seed-network to id
        list_id_crawl = ['%s:seed:seed_network_by_sentifi_score' % tw_id for tw_id in list_id_crawl]

        if list_id_crawl:
            self.post_to_follower_following_queue(list_id_crawl)

        # now we have following and follower to do it
        # list_id_following_follower = self.crawl_following_follower(list_id_crawl)
        # if list_id_following_follower:
        #         self.post_to_queue(list_id_following_follower)
        #         self.insert_user_source(list_id_following_follower, 10)

        self.logger.info('End crawl seed_network_by_sentifi_score...')
        self.con_da0.close()

    @staticmethod
    def get_top_rank():
        '''
        Get top rank publisher by category in elasticsearch
        '''
        client_es = my_connection.es_connection()

        list_cat = ['549d3b36830f780155c90c11', '549d3b36830f780155c90b59','549d3b36830f780155c90c20', '549d3b36830f780155c90c21', '549d3b36830f780155c90c22', '549d3b36830f780155c90c23', '549d3b36830f780155c90c19']
        list_id_crawl = []
        for cat in list_cat:
            query = {
                "fields": [
                  "channel_meta.sns_id"
               ],
                "query": {
                    "term": {
                       "filters.categories.mongo_id":  cat
                    }
                },
                "size": 100,
                "sort": [
                   {
                      "score.s_sentifi": {
                         "order": "desc"
                      }
                   }
                ]
            }
            res = client_es.search(
                index="ps_cur_week",
                doc_type="ps",
                body=query,
                request_timeout=36000,
            )
            for user_score in res['hits']['hits']:
                list_id_crawl.append(int(user_score['_id'].split('-')[1]))

        return list_id_crawl

    def crawl_list(self, list_id_crawl):
        '''
        crawl list twitter of publisher, insert to da0
        '''

        cur_da0 = self.con_da0.cursor()

        list_id_crawl_profile = []

        for user_id in list_id_crawl:
            lists = self.twitter_service.get_lists_by_id(user_id)
            subscriptions = self.twitter_service.get_subscriptions_by_id(user_id)

            for li in lists:
                #save to list
                cur_da0.execute('''
                    UPDATE twitter.list
                    SET payload = %s, updated_at = now()
                    WHERE list_id = %s
                ''', [json.dumps(li), li['id']])
                if not cur_da0.rowcount:
                    cur_da0.execute('''
                        INSERT INTO twitter.list(list_id, payload)
                        VALUES(%s, %s)
                    ''', [li['id'], json.dumps(li)])
                    self.con_da0.commit()

                #insert user_list
                cur_da0.execute('''
                    INSERT INTO twitter.user_list(user_id, list_id, label)
                    SELECT %s, %s, 'c'
                    WHERE NOT EXISTS(
                        SELECT user_id, list_id
                         FROM twitter.user_list
                         WHERE user_id = %s
                         AND list_id = %s
                    )
                ''', [li['user']['id'], li['id'], li['user']['id'], li['id']])
                self.con_da0.commit()

                cur_da0.execute('''
                    INSERT INTO twitter.user_list(user_id, list_id, label)
                    SELECT %s, %s, 'c'
                    WHERE NOT EXISTS(
                        SELECT user_id, list_id
                         FROM twitter.user_list
                         WHERE user_id = %s
                         AND list_id = %s
                    )
                ''', [int(user_id), li['id'], int(user_id), li['id']])

                list_id_crawl_profile.append(li['user']['id'])
            for sub in subscriptions:
                #save to user_list
                cur_da0.execute('''
                    UPDATE twitter.list
                    SET payload = %s, updated_at = now()
                    WHERE list_id = %s
                ''', [json.dumps(sub), sub['id']])
                if not cur_da0.rowcount:
                    cur_da0.execute('''
                        INSERT INTO twitter.list(list_id, payload)
                        VALUES(%s, %s)
                    ''', [sub['id'], json.dumps(sub)])
                    self.con_da0.commit()

                #insert user_list
                cur_da0.execute('''
                    INSERT INTO twitter.user_list(user_id, list_id, label)
                    SELECT %s, %s, 'c'
                    WHERE NOT EXISTS(
                        SELECT user_id, list_id
                         FROM twitter.user_list
                         WHERE user_id = %s
                         AND list_id = %s
                    )
                ''', [sub['user']['id'], sub['id'], sub['user']['id'], sub['id']])
                self.con_da0.commit()

                cur_da0.execute('''
                    INSERT INTO twitter.user_list(user_id, list_id, label)
                    SELECT %s, %s, 'c'
                    WHERE NOT EXISTS(
                        SELECT user_id, list_id
                         FROM twitter.user_list
                         WHERE user_id = %s
                         AND list_id = %s
                    )
                ''', [int(user_id), sub['id'], int(user_id), sub['id']])

                list_id_crawl_profile.append(sub['user']['id'])

            return list_id_crawl_profile

    def crawl_following_follower(self, list_id_crawl):
        list_id_folowing_follower = []
        for each_id in list_id_crawl:
            list_id_folowing_follower += self.twitter_service.get_follower(user_id=each_id)
            list_id_folowing_follower += self.twitter_service.get_following(user_id=each_id)

        return list_id_folowing_follower

    def post_to_queue(self, list_id_crawl):
        queue = Queue({
            'uri': ['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
            'name': 'DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishers'})

        for user_id in list_id_crawl:
            try:
                queue.post(str(user_id))
            except Exception as e:
                self.logger.error('Post user id to queue '
                                  'DailyBatchTwitterProfileCrawlByTwitterId_PotentialPublishersExchange error: %s' % e)
                pass

        queue.close()

    def post_to_follower_following_queue(self, list_id_crawl):
        queue = Queue({
            'uri': ['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
                    'name': 'FollowersFollowingCrawlByTwitter_PotentialPublishers'})

        for user_id in list_id_crawl:
            try:
                queue.post(str(user_id))
            except Exception as e:
                self.logger.error('Post user id to queue '
                                  'FollowersFollowingCrawlByTwitter_PotentialPublishers error: %s' % e)
                pass

        queue.close()

    def post_to_crawl_profile_queue(self, list_id_crawl):
        queue = Queue({
            'uri': ['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
                    'name': 'TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers'})

        for user_id in list_id_crawl:
            try:
                queue.post(str(user_id))
            except Exception as e:
                self.logger.error('Post user id to queue '
                                  'TwitterProfileCrawlByTwitterIdProcessor_PotentialPublishers error: %s' % e)
                pass

        queue.close()

    def insert_user_source(self, list_id, source_id):
        '''
        insert list ids into source after check exist, using postgresql copy_from
        '''
        cur_da0 = self.con_da0.cursor()
        threshold = 1000
        start = 0
        while start < len(list_id):
            len_threshold_list = min(threshold, len(list_id) - start)
            # Check exist ids in user_source
            new_list_id = set()
            cur_da0.execute('''
                SELECT user_id
                FROM user_source
                WHERE user_id = ANY(%s)
                    AND source_id = %s
            ''', [list_id[start: start + len_threshold_list], source_id])
            for item, in cur_da0:
                new_list_id.add(item)

            new_list_id = set(list_id[start: start + len_threshold_list]) - new_list_id

            cpy = BytesIO()
            for id in new_list_id:
                cpy.write(str(id) + '\t' + str(source_id) + '\n')

            cpy.seek(0)
            cur_da0.copy_from(cpy, 'user_source', columns=('user_id', 'source_id'))
            self.con_da0.commit()

            start += threshold


if __name__ == '__main__':
    crawl_top_rank = CrawlTopRank()
    crawl_top_rank.process()
