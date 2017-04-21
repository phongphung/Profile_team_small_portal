__author__ = 'sunary'


from utils import my_connection


class UpdatePublisherNeedAvatar():

    def __init__(self):
        self.con_psql1 = my_connection.psql1_connection()
        self.con_dev = my_connection.dev_connection('nhat')

    def get_publisher(self):
        cur_psql1 = self.con_psql1.cursor()
        cur_dev = self.con_dev.cursor()

        cur_dev.execute('''
            SELECT object_id, channel
            from public.new_blog_need_avatar
        ''')

        for doc in cur_dev:
            # cur_psql1.execute('''
            #     SELECT object_payload->>'name'
            #     FROM mongo.mg_publisher
            #             INNER JOIN mongo.mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
            #     WHERE sns_name = %s
            #             AND object_id = %s
            # ''', [doc[1], doc[0]])
            # name = cur_psql1.fetchone()
            # if name:
            #     self.update_name(doc[0], name[0])
            cur_psql1.execute('''
                SELECT object_payload->>'url'
                FROM mongo.mg_news
                WHERE object_id IN (
                    SELECT sns_id
                    FROM mg_publisher_sns_clean
                    WHERE publisher_mongo_id = %s)
            ''', [doc[0]])
            url = cur_psql1.fetchone()
            if url:
                self.update_url(doc[0], url[0])

    def update_name(self, object_id, name):
        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            UPDATE public.new_blog_need_avatar
            SET name = %s
            WHERE object_id = %s
        ''', [name, object_id])
        self.con_dev.commit()

    def update_url(self, object_id, url):
        cur_dev = self.con_dev.cursor()
        cur_dev.execute('''
            UPDATE public.new_blog_need_avatar
            SET url = %s
            WHERE object_id = %s
        ''', [url, object_id])
        self.con_dev.commit()

    def close_connection(self):
        self.con_psql1.close()
        self.con_dev.close()

if __name__ == '__main__':
    get_name = UpdatePublisherNeedAvatar()
    get_name.get_publisher()
    get_name.close_connection()
