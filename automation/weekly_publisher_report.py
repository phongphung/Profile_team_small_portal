__author__ = 'sunary'


from utils import my_helper, my_connection
from automation.kpi_from_db import KPIFromDb


class WeeklyPublisherReport():

    def __init__(self):
        self.con_psql1 = my_connection.psql1_connection()
        self.logger = my_helper.init_logger(self.__class__.__name__)

    def avatar_status(self):
        con_dev = my_connection.dev_connection('nhat')
        cur_dev = con_dev.cursor()

        cur_dev.execute('''
            SELECT COUNT(*)
            FROM twitter_need_avatar
            WHERE NOT missing_avatar
        ''')
        self.twitter_broken_avatar = cur_dev.fetchone()[0]

        cur_dev.execute('''
            SELECT COUNT(*)
            FROM twitter_need_avatar
            WHERE missing_avatar
        ''')
        self.twitter_missing_avatar = cur_dev.fetchone()[0]
        con_dev.close()

    def iso_code_status(self):
        cur_psql1 = self.con_psql1.cursor()

        cur_psql1.execute('''
            SELECT COUNT(*)
            FROM public.sns_account
            WHERE sns_name = 'tw'
                AND country_code IS NULL
        ''')
        self.con_psql1.commit()
        self.twitter_missing_iso = cur_psql1.fetchone()[0]

    def big_avatar(self):
        cur_psql1 = self.con_psql1.cursor()

        self.twitter_total = 0
        self.twitter_big_avatar = 0
        last_sns_id = ''
        threshold = 50000
        while True:
            cur_psql1.execute('''
                SELECT sns_id, payload->>'image'
                FROM public.sns_account
                WHERE sns_name = 'tw'
                    AND sns_id > %s
                ORDER BY sns_id
                LIMIT %s
            ''', (str(last_sns_id), threshold))
            self.con_psql1.commit()
            rows = cur_psql1.fetchall()
            last_sns_id = str(rows[len(rows) - 1][0])
            if rows:
                self.twitter_total += 1
                for doc in rows:
                    if doc[1] and '_400x400.' in str(doc[1]):
                        self.twitter_big_avatar += 1
            else:
                break
        self.logger.info('twitter_total: %s' % (self.twitter_total))
        self.logger.info('twitter_big_avatar: %s' % (self.twitter_big_avatar))

    def twitter_itemkey_status(self):
        cur_psql1 = self.con_psql1.cursor()

        # cur_psql1.execute('''
        #     WITH twitter_having_multi_item AS (
        #         SELECT sns_id
        #           FROM public.sns_account i_s
        #           WHERE sns_name = 'tw'
        #             AND status = 1
        #           GROUP BY sns_id
        #           HAVING COUNT(*) > 1
        #     )
        #     SELECT COUNT(DISTINCT sns_id)
        #       FROM twitter_having_multi_item
        # ''')
        # self.con_psql1.commit()
        self.twitter_has_many_itemkey = 0  # cur_psql1.fetchone()[0]

        # cur_psql1.execute('''
        #     WITH twitter_having_multi_publisher AS (
        #         SELECT sns_id
        #           FROM public.sns_account ps
        #           WHERE sns_name = 'tw'
        #           GROUP BY sns_id
        #           HAVING COUNT(*) > 1
        #     )
        #     SELECT COUNT(DISTINCT sns_id)
        #       FROM twitter_having_multi_publisher
        # ''')
        # self.con_psql1.commit()
        self.twitter_has_many_publisher = 0  # cur_psql1.fetchone()[0]

        cur_psql1.execute('''
            SELECT COUNT(sns_id)
            FROM public.sns_account
            WHERE sns_name = 'tw' AND named_entity_id IS NULL
        ''')
        self.con_psql1.commit()
        self.twitter_without_itemkey = cur_psql1.fetchone()[0]

    def close_connection(self):
        self.con_psql1.close()

    def insert_db(self):
        con_dev = my_connection.dev_connection('nhat')
        list_id = [KPIFromDb.ID_TWITTER_BROKEN_AVATAR, KPIFromDb.ID_TWITTER_MISSING_AVATAR, KPIFromDb.ID_TWITTER_MISSING_ISO, KPIFromDb.ID_TWITTER_HAS_MANY_ITEMKEY, KPIFromDb.ID_TWITTER_HAS_MANY_PUBLISHER, KPIFromDb.ID_TWITTER_WITHOUT_ITEMKEY]
        list_value = [self.twitter_broken_avatar, self.twitter_missing_avatar, self.twitter_missing_iso, self.twitter_has_many_itemkey, self.twitter_has_many_publisher, self.twitter_without_itemkey]

        cur_dev = con_dev.cursor()
        for i in range(len(list_id)):
            cur_dev.execute('''
                INSERT INTO public.kpi_report(id, created_at, value)
                VALUES(%s, now(), %s)
            ''', [list_id[i], list_value[i]])
            con_dev.commit()
        con_dev.close()

if __name__ == '__main__':
    weekly_publisher_report = WeeklyPublisherReport()
    weekly_publisher_report.avatar_status()
    weekly_publisher_report.iso_code_status()
    # weekly_publisher_report.big_avatar()
    weekly_publisher_report.twitter_itemkey_status()
    weekly_publisher_report.close_connection()
    weekly_publisher_report.insert_db()
