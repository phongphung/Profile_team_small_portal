__author__ = 'sunary'


import json
from utils import my_helper, my_connection


class ChangeTwitterBigAvatar():

    def __init__(self):
        self.con_psql0 = my_connection.psql0_connection()

    def process(self):
        '''
        Get all image url from con_psql1, check exist url, replace if needed
        '''
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        last_object_id = ''
        threshold = 50000
        while True:
            cur_psql1.execute('''
                SELECT object_id,
                        object_payload->>'image'
                FROM mg_publisher
                    INNER JOIN mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
                WHERE sns_name = 'tw'
                    AND object_id > %s
                ODER BY object_id
                LIMIT %s
            ''', [last_object_id, threshold])
            if cur_psql1.rowcount:
                for doc in cur_psql1:
                    if doc[0] > last_object_id:
                        last_object_id = doc[0]
                    if doc[1] and my_helper.check_exist_url(doc[1]) and ('_400x400.' not in doc[1]):
                        new_image = self.change_big_avatar(doc[1])
                        if new_image:
                            self.update_avatar(doc[0], new_image)
            else:
                break
        con_psql1.close()

    def change_big_avatar(self, image):
        '''
        Change to high resolution avatar: replace normal or bigger by 400x400

        Args:
            image: image url
        '''
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
            FROM mg_publisher
            WHERE object_id = %s
        ''', [object_id])

        if cur_psql0.rowcount:
            object_payload = cur_psql0.fetchone()[0]
            object_payload['image'] = image

            cur_psql0.execute('''
                UPDATE mg_publisher
                SET object_payload = %s, updated_at = now()
                WHERE object_id = %s
            ''', [json.dumps(object_payload), object_id])

            self.con_psql0.commit()

    def close_connection(self):
        self.con_psql0.close()

if __name__ == '__main__':
    change_avatar = ChangeTwitterBigAvatar()
    change_avatar.process()
    change_avatar.close_connection()