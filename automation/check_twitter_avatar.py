__author__ = 'sunary'


from utils import my_connection
from utils.my_mongo import Mongodb
import json
from utils import my_helper


class ChangeTwitterAvatar():

    def __init__(self):
        self.con_psql0 = my_connection.psql0_connection()
        self.con_dev = my_connection.dev_connection('nhat')
        self.mongo6 = Mongodb(host='mongo6.ssh.sentifi.com',
                              db='Twitter',
                              col='User')

        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            DROP TABLE twitter_need_avatar
        ''')
        self.con_dev.commit()

        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            CREATE TABLE twitter_need_avatar
            (object_id char(24),
            user_id bigint,
            missing_avatar boolean,
            created_at timestamp)
        ''')
        self.con_dev.commit()

    def process_one_id(self, object_id):
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        cur_psql1.execute('''
            SELECT object_id,
                    sns_id,
                    object_payload->>'image'
            FROM mongo.mg_publisher
                INNER JOIN mongo.mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
            WHERE sns_name = 'tw'
                AND object_id = %s
        ''', [object_id])
        if cur_psql1.rowcount:
            for doc in cur_psql1:
                if doc[2]:
                    if not my_helper.check_exist_url(doc[2]):
                        new_image = self.check_avatar_in_mongo6(doc[1], doc[2])
                        if new_image:
                            self.update_avatar(doc[0], new_image)
                        else:
                            self.save_need_avatar(doc[0], doc[1], False)
                else:
                    new_image = self.check_avatar_in_mongo6(doc[1], doc[2])
                    if new_image:
                        self.update_avatar(doc[0], new_image)
                    else:
                        self.save_need_avatar(doc[0], doc[1], True)
        con_psql1.close()

    def process(self):
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        last_object_id = ''
        threshold = 50000
        while True:
            cur_psql1.execute('''
                SELECT object_id,
                        sns_id,
                        object_payload->>'image'
                FROM mongo.mg_publisher
                    INNER JOIN mongo.mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
                WHERE sns_name = 'tw'
                    AND object_id > %s
                ORDER BY object_id
                LIMIT %s
            ''', [last_object_id, threshold])
            if cur_psql1.rowcount:
                for doc in cur_psql1:
                    if doc[0] > last_object_id:
                        last_object_id = doc[0]
                    if doc[2]:
                        if not my_helper.check_exist_url(doc[2]):
                            new_image = self.check_avatar_in_mongo6(doc[1], doc[2])
                            if new_image:
                                self.update_avatar(doc[0], new_image)
                            else:
                                self.save_need_avatar(doc[0], doc[1], False)
                    else:
                        new_image = self.check_avatar_in_mongo6(doc[1], doc[2])
                        if new_image:
                            self.update_avatar(doc[0], new_image)
                        else:
                            self.save_need_avatar(doc[0], doc[1], True)
            else:
                break
        con_psql1.close()

    def check_avatar_in_mongo6(self, id, image):
        res = self.mongo6.find({'id_str': id}, ['profile_image_url_https'], sort=[('_id', -1)], limit=1)
        if res:
            try:
                url = res.next()['profile_image_url_https']
                if url and url != image and my_helper.check_exist_url(url):
                    big_image = self.change_big_avatar(url)
                    return big_image if big_image else url
            except:
                return ''
        return ''

    def change_big_avatar(self, image):
        if '_normal.' in image:
            image = image.replace('_normal.', '_400x400.')
        elif '_bigger.' in image:
            image = image.replace('_bigger.', '_400x400.')

        if '_400x400.' in image and my_helper.check_exist_url(image):
            return image

        return ''

    def update_avatar(self, object_id, image):
        cur_psql0 = self.con_psql0.cursor()

        cur_psql0.execute('''
            SELECT object_payload
            FROM mongo.mg_publisher
            WHERE object_id = %s
        ''', [object_id])

        if cur_psql0.rowcount:
            object_payload = cur_psql0.fetchone()[0]
            object_payload['image'] = image

            cur_psql0.execute('''
                UPDATE mongo.mg_publisher
                SET object_payload = %s, updated_at = now()
                WHERE object_id = %s
            ''', [json.dumps(object_payload), object_id])

            self.con_psql0.commit()

    def save_need_avatar(self, object_id, id, missing_avatar):
        cur_dev = self.con_dev.cursor()

        cur_dev.execute('''
            INSERT INTO public.twitter_need_avatar(object_id, user_id, missing_avatar, created_at)
            VALUES(%s, %s, %s, now())
        ''', [object_id, int(id), missing_avatar])

        self.con_dev.commit()

    def close_connection(self):
        self.con_psql0.close()
        self.con_dev.close()


if __name__ == '__main__':
    check_avatar = ChangeTwitterAvatar()
    # check_avatar.process_one_id('53b3f3596f01079ab7e9ffe1')
    # print check_avatar.check_avatar_in_mongo6('929983970', '')
    check_avatar.process()
    check_avatar.close_connection()