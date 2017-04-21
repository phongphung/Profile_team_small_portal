__author__ = 'sunary'


import redis
import os
import pandas as pd
from utils.my_mongo import Mongodb
from utils import my_datetime, my_connection
from twitter.twitter_services import TwitterService
import time
from queue.queue import Queue


class GetFolowerFollowingProfile():

    def __init__(self):
        self.list_id = self.get_dach_seed()
        self.mongodb = Mongodb(db='follower_following',
                               col=my_datetime.name_append_date('profile'))

        self.list_keyword_ir = ['investor relation', 'investor-relation', 'ir professional', 'ir specialist','ir director', 'ir services', 'ir consultant','ir analyst', 'ir executive', 'ir team', 'iragency']
        self.list_keyword_cc = ['corporate communication', 'corporate comm', 'corp comm']

    def connect_db(self):
        self.con_da0 = my_connection.da0_connection()
        self.con_psql1 = my_connection.psql1_connection()

    def print_id(self):
        pd_file = pd.read_csv('/home/nhat/data/40 subcribers Twitter ID.csv')

        str_ids = '('
        for id in pd_file['Twitter ID']:
            str_ids = str_ids + "'" + str(id) + "',"

        print str_ids + ')'

    def print_screenname_to_id(self):
        pd_file = pd.read_csv('/home/sunary/data_report/24june2015_ 51Twitter Account( following follower IR)_24june2015by Cam.csv')

        screen_names = []
        for i in pd_file['twitter_ir']:
            screen_names.append(str(i))

        service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        ids =  service.get_ids_by_screen_names(screen_names)

        str_ids = '('
        for id in ids:
            str_ids = str_ids + "'" + str(id) + "',"
        print str_ids + ')'


    def get_from_list(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode
            FROM twitter.tw_user u
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE u.user_id IN %s
        ''', [self.list_id])

        cur_psql1 = self.con_psql1.cursor()

        for profile in  cur_da0:
            cur_psql1.execute('''
                SELECT True
                FROM sns_account
                WHERE sns_id = %s
                    AND sns_name = 'tw'
            ''', [str(profile[0])])

            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': True if cur_psql1.rowcount else False,
                    }

            self.mongodb.insert(data)

    def create_list_follower_following(self):
        cur_da0 = self.con_da0.cursor()

        #empty db
        cur_da0.execute('''
            DELETE FROM twitter._fft
        ''')
        self.con_da0.commit()

        #create lis follower
        cur_da0.execute('''
            INSERT INTO _fft(user_id, follower)

            SELECT user_id,
            FROM follower
            WHERE
            UNION(
                SELECT follower_id
                FROM follower
                WHERE
            )
        ''')
        self.con_da0.commit()

    def check_dup(self):
        '''
        check dup before insert
        '''
        self.mongodb.create_index('id')
        self.get_follower()
        self.get_following()

    def get_follower(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode As isocode,
                ut.status = 1 AS exist_db,
                f.user_id AS follower
            FROM twitter.tw_user u
                inner join twitter.follower AS f ON u.user_id = f.follower_id
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
                left join twitter.user_tracking AS ut ON u.user_id = ut.user_id
            WHERE f.user_id = ANY(%s)
        ''', (self.list_id,))

        for profile in cur_da0:
            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': profile[11],
                    'follower': profile[12],
                    'following': ''
                    }

            res_mongo = self.mongodb.find_one({'id': profile[0]})
            if res_mongo:
                new_follower = str(res_mongo.get('follower')) + '-' + str(profile[12])
                self.mongodb.update({'id': profile[0]}, {'follower': new_follower}, upsert=False)
            else:
                self.mongodb.insert(data)

    def get_following(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode,
                ut.status = 1 AS exist_db,
                f.follower_id AS following
            FROM twitter.tw_user u
                inner join twitter.follower AS f ON u.user_id = f.user_id
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
                left join twitter.user_tracking AS ut ON u.user_id = ut.user_id
            WHERE f.follower_id = ANY(%s)
        ''', (self.list_id,))

        for profile in cur_da0:
            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': profile[11],
                    'follower': '',
                    'following': profile[12]
                    }
            res_mongo = self.mongodb.find_one({'id': profile[0]})
            if res_mongo:
                new_following = str(res_mongo.get('following')) + '-' + str(profile[12])
                self.mongodb.update({'id': profile[0]}, {'following': new_following}, upsert=False)
            else:
                self.mongodb.insert(data)

    def not_check_dup(self):
        '''
        insert without check dup
        '''
        self.get_follower_not_check_duplicate()
        self.get_following_not_check_duplicate()

    def get_follower_not_check_duplicate(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode,
                f.user_id AS follower
            FROM twitter.tw_user u
                inner join twitter.follower AS f ON u.user_id = f.follower_id
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE f.user_id IN %s
        ''', [self.list_id])

        cur_psql1 = self.con_psql1.cursor()
        for profile in  cur_da0:
            #check exist in psql1
            cur_psql1.execute('''
                SELECT True
                FROM mg_publisher_sns_clean
                WHERE sns_id = %s
                    AND sns_name = 'tw'
            ''', [str(profile[0])])
            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': True if cur_psql1.rowcount else False,
                    'follower': profile[11],
                    'following': ''
                    }

            self.mongodb.insert(data)

    def get_following_not_check_duplicate(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' As profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode,
                f.follower_id AS following
            FROM twitter.tw_user u
                INNER JOIN twitter.follower AS f ON u.user_id = f.user_id
                LEFT JOIN twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE f.follower_id IN %s
        ''', [self.list_id])

        cur_psql1 = self.con_psql1.cursor()
        for profile in  cur_da0:
            #check exist in psql1
            cur_psql1.execute('''
                SELECT True
                FROM mg_publisher_sns_clean
                WHERE sns_id = %s
                    AND sns_name = 'tw'
            ''', [str(profile[0])])
            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': True if cur_psql1.rowcount else False,
                    'follower': '',
                    'following': profile[11]
                    }
            self.mongodb.insert(data)

    def get_score(self):
        # self.redis = redis.StrictRedis(host='127.0.0.1', port=6379, db=10)
        self.redis = redis.Redis('localhost')
        self.get_seed_score()
        # self.get_follower_score()
        # self.get_following_score()

    def get_seed_score(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode
            FROM twitter.tw_user u
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE u.user_id IN %s
        ''', [self.list_id])

        cur_psql1 = self.con_psql1.cursor()
        for profile in  cur_da0:
            #check exist in psql1
            cur_psql1.execute('''
                SELECT True
                FROM mg_publisher_sns_clean
                WHERE sns_id = %s
                    AND sns_name = 'tw'
            ''', [str(profile[0])])
            score = ''
            if cur_psql1.rowcount:
                score = self.redis.get('sns:tw-%s:score' % (profile[0]))

            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': True if cur_psql1.rowcount else False,
                    'score': score
                    }

            self.mongodb.insert(data)

    def get_follower_score(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode
            FROM twitter.tw_user u
                inner join twitter.follower AS f ON u.user_id = f.follower_id
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE f.user_id IN %s
        ''', [self.list_id])

        cur_psql1 = self.con_psql1.cursor()
        for profile in  cur_da0:
            #check exist in psql1
            cur_psql1.execute('''
                SELECT True
                FROM mg_publisher_sns_clean
                WHERE sns_id = %s
                    AND sns_name = 'tw'
            ''', [str(profile[0])])
            score = ''
            if cur_psql1.rowcount:
                score = self.redis.get('sns:tw-%s:score' % (profile[0]))

            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'exist_db': True if cur_psql1.rowcount else False,
                    'score': score
                    }

            self.mongodb.insert(data)

    def get_following_score(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' As profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode,
                f.follower_id AS following
            FROM twitter.tw_user u
                INNER JOIN twitter.follower AS f ON u.user_id = f.user_id
                LEFT JOIN twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE f.follower_id IN %s
        ''', [self.list_id])

        cur_psql1 = self.con_psql1.cursor()
        for profile in  cur_da0:
            #check exist in psql1
            cur_psql1.execute('''
                SELECT True
                FROM mg_publisher_sns_clean
                WHERE sns_id = %s
                    AND sns_name = 'tw'
            ''', [str(profile[0])])
            score = ''
            if cur_psql1.rowcount:
                score = self.redis.get('sns:tw-%s:score' % (profile[0]))

            data = {'id': profile[0],
                    'name': profile[1],
                    'screen_name': profile[2],
                    'description': profile[3],
                    'location': profile[4],
                    'url': profile[5],
                    'lang': profile[6],
                    'statuses_count': profile[7],
                    'profile_image_url': profile[8],
                    'time_zone': profile[9],
                    'isocode': profile[10],
                    'following': profile[11],
                    'exist_db': True if cur_psql1.rowcount else False,
                    'score': score
                    }
            self.mongodb.insert(data)

    def get_follower_following_by_date(self):
        self.mongodb = Mongodb(db= 'follower_following', col= my_datetime.name_append_date('profile_cam2'))
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT u.user_id,
                u.payload->>'name' AS name,
                u.payload->>'screen_name' AS screen_name,
                u.payload->>'description' AS description,
                u.payload->>'location' AS location,
                u.payload->'entities'->'url'->'urls'->0->>'expanded_url' AS url,
                u.payload->>'lang' AS lang,
                u.payload->>'statuses_count' AS statuses_count,
                u.payload->>'profile_image_url' AS profile_image_url,
                u.payload->>'time_zone' AS time_zone,
                i.isocode AS isocode
            FROM twitter.tw_user u
                left join twitter.user_isocode AS i ON u.user_id = i.user_id
            WHERE u.created_at > %s
        ''', ['2015-07-02'])

        cur_psql1 = self.con_psql1.cursor()
        for profile in  cur_da0:
            if profile[3] and (any(keyword in profile[3].lower() for keyword in self.list_keyword_ir) or
                    any(keyword in profile[3].lower() for keyword in self.list_keyword_cc) or
                    (' ir 'in profile[1].lower() or profile[1].lower().startswith('ir '))):
                #check exist in psql1
                cur_psql1.execute('''
                    SELECT True
                    FROM mg_publisher_sns_clean
                    WHERE sns_id = %s
                        AND sns_name = 'tw'
                ''', [str(profile[0])])
                data = {'id': profile[0],
                        'name': profile[1],
                        'screen_name': profile[2],
                        'description': profile[3],
                        'location': profile[4],
                        'url': profile[5],
                        'lang': profile[6],
                        'statuses_count': profile[7],
                        'profile_image_url': profile[8],
                        'time_zone': profile[9],
                        'isocode': profile[10],
                        'exist_db': True if cur_psql1.rowcount else False
                        }

                self.mongodb.insert(data)

    def post_follower_following_to_queue(self):
        cur_da0 = self.con_da0.cursor()
        queue = Queue(dict(
            Uri='worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'
        ), queue_name='FollowersFollowingCrawlByTwitter_PotentialPublishers')


        cur_da0.execute('''
            SELECT payload->>'name',
                    payload->>'description'
            FROM twitter.tw_user
            WHERE created_at > %s
                AND created_at < %s
        ''', ['2015-07-02', '2015-07-03'])

        num_user = 0
        for profile in  cur_da0:
            if profile[1] and (any(keyword in profile[1].lower() for keyword in self.list_keyword_ir) or
                    any(keyword in profile[1].lower() for keyword in self.list_keyword_cc) or
                    (' ir 'in profile[0].lower() or profile[0].lower().startswith('ir '))):
                num_user += 1
                try:
                    print profile[0]
                    queue.post(profile[0])
                    time.sleep(1)
                except:
                    pass
        queue.close()
        print num_user

    def close_connection(self):
        self.con_da0.close()
        self.con_psql1.close()

    def get_dach_seed(self):
        df = pd.read_excel('/home/diep.dao/crawl_data/151016_DACH_seed_flow_crawling/151016_DACH_seed.xls')
        screen_names = df['screen_name'].tolist()
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        id_strs = twitter_service.get_ids_by_screen_names(list_screen_names=screen_names)
        print 'screen_names/id_strs: %d/%d' % (len(screen_names), len(id_strs))
        return [int(_) for _ in id_strs]

    def export_follower_following(self):
        self.mongodb.export_excel(fields=['id', 'name', 'screen_name', 'description', 'location', 'url', 'lang',
                                          'statuses_count', 'profile_image_url', 'time_zone', 'isocode', 'exist_db',
                                          'follower', 'following'],
                                  output='~/crawl_data/151016_DACH_seed_flow_crawling/151016_DACH_seed_follower_following.xlsx')


if __name__ == '__main__':
    '''
    Exports:
        $ mongoexport -d follower_following -c profile_cam_20150624 --csv -o follower_following.csv --fieldFile field_follower_following.txt
        $ scp nhat.vo@dev.ssh.sentifi.com:/home/nhat.vo/follower_following.csv .
    mongoexport -d follower_following -c profile_20151020 --csv -o ~/crawl_data/151016_DACH_seed_flow_crawling/151016_DACH_seed_follower_following.csv -f id,name,screen_name,description,location,url,lang,statuses_count,profile_image_url,time_zone,isocode,exist_db,follower,following
    '''
    get_profile = GetFolowerFollowingProfile()
    # get_profile.connect_db()
    # get_profile.check_dup()
    # get_profile.close_connection()
    get_profile.export_follower_following()
