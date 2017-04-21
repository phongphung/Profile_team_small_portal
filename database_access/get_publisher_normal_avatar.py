__author__ = 'sunary'


import json
from utils import my_helper, my_connection
from utils.my_mongo import Mongodb
import pandas as pd


class GetPublisherNormalAvatar():

    def __init__(self):
        pass

    def normal_avatar(self):
        '''
        get normal avatar if image don't contain '_400x400'
        '''
        mongo_normal_avatar = Mongodb(db='normal_avatar', col='image')

        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        self.twitter_total = 0
        self.twitter_big_avatar = 0
        last_object_id = ''
        threshold = 50000
        while True:
            cur_psql1.execute('''
                SELECT p.object_id,
                        p.object_payload->>'image',
                        t.object_payload->>'id_str',
                        t.object_payload->>'screen_name'
                FROM mongo.mg_publisher_sns_clean ps
                    INNER JOIN
                        mongo.mg_publisher p ON (ps.publisher_mongo_id = p.object_id)
                    INNER JOIN
                        mongo.mg_twitter t ON (ps.sns_id = t.object_payload->>'id_str')
                WHERE ps.sns_name = 'tw'
                    AND p.object_id > %s
                ORDER BY p.object_id
                LIMIT %s
            ''', [last_object_id, threshold])
            if cur_psql1.rowcount:
                for doc in cur_psql1:
                    if doc[0] > last_object_id:
                        last_object_id = doc[0]
                    if doc[1] and '_400x400.' not in str(doc[1]):
                        mongo_normal_avatar.insert({'object_id': doc[0],
                                                   'image': doc[1],
                                                   'valid_image': 1 if my_helper.check_exist_url(doc[1]) else 0,
                                                   'id': doc[2],
                                                   'screen_name': doc[3],
                                                   })
            else:
                break
        con_psql1.close()

    def update_big_avatar(self):
        '''
        read big avatar in pd_file then update
        '''
        self.con_psql0 = my_connection.psql0_connection()

        pd_file = pd.read_csv('/home/nhat/data/Avatar_Twitter_ (file 1-4)_20July2015_Duy.csv')
        for i in range(len(pd_file['photo'])):
            if my_helper.check_exist_url(pd_file['photo'][i]):
                user_id = pd_file['tw-general'][i].split('{')[1][:-1]
                self.update_avatar(user_id, pd_file['photo'][i])

        self.con_psql0.close()

    def update_avatar(self, user_id, image):
        '''
        Update avatar in object_payload
        '''
        cur_psql0 = self.con_psql0.cursor()
        cur_psql0.execute('''
            SELECT p.object_payload, p.object_id
            FROM mongo.mg_publisher p
                INNER JOIN mongo.mg_twitter t ON (p.object_id = t.object_id)
            WHERE t.object_payload->>'id_str' = %s
        ''', [user_id])

        if cur_psql0.rowcount:
            object_payload, object_id = cur_psql0.fetchone()
            object_payload['image'] = image

            cur_psql0.execute('''
                UPDATE mongo.mg_publisher
                SET object_payload = %s, updated_at = now()
                WHERE object_id = %s
            ''', [json.dumps(object_payload), object_id])

            self.con_psql0.commit()

if __name__ == '__main__':
    get_normal_avatar = GetPublisherNormalAvatar()
    get_normal_avatar.update_big_avatar()


