__author__ = 'sunary'


from utils import my_helper, my_connection


class CheckNewBlogAvatar():

    def __init__(self):
        self.con_psql1 = my_connection.psql1_connection()
        self.con_dev = my_connection.dev_connection('nhat')

        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            DROP TABLE new_blog_need_avatar
        ''')
        self.con_dev.commit()

        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            CREATE TABLE new_blog_need_avatar
            (object_id char(24),
            url text,
            name text,
            channel text,
            created_at timestamp)
        ''')
        self.con_dev.commit()

    def process(self):
        cur_psql1 = self.con_psql1.cursor()

        last_object_id = ''
        threshold = 50000
        while True:
            cur_psql1.execute('''
                SELECT p.object_id,
                n.object_payload ->>'url',
                p.object_payload->>'name',
                p.object_payload->>'image'
                FROM mongo.mg_publisher_sns_clean ps
                    INNER JOIN mongo.mg_publisher p ON (p.object_id = ps.publisher_mongo_id)
                    INNER JOIN mongo.mg_news n ON (ps.sns_id = n.object_id)
                WHERE ps.sns_name = 'news'
                    AND p.object_id > %s
                ORDER BY p.object_id
                LIMIT %s
            ''', [last_object_id, threshold])
            if cur_psql1.rowcount:
                for doc in cur_psql1:
                    if doc[0] > last_object_id:
                        last_object_id = doc[0]
                    if not (doc[3] and my_helper.check_exist_url(doc[3])):
                        self.save_need_avatar(doc[0], doc[1], doc[2], 'news')
            else:
                break
        self.close_connection()

    def save_need_avatar(self, object_id, url, name, channel):
        cur_dev = self.con_dev.cursor()

        cur_dev.execute('''
            INSERT INTO public.new_blog_need_avatar(object_id, url, name, channel, created_at)
            VALUES(%s, %s, %s, now())
        ''', [object_id, url, name, channel])

        self.con_dev.commit()

    def close_connection(self):
        self.con_psql1.close()
        self.con_dev.close()


if __name__ == '__main__':
    check_avatar = CheckNewBlogAvatar()
    check_avatar.process()