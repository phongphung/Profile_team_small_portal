__author__ = 'sunary'


from io import BytesIO
from utils import my_helper, my_connection
from automation.kpi_from_db import KPIFromDb
from utils import my_datetime


class WeeklySourceReport():

    def __init__(self):
        self.logger = my_helper.init_logger(self.__class__.__name__)
        self.client_ES = my_connection.es_connection()

        # connect da0
        self.con_da0 = my_connection.da0_connection()

        # table 1
        self.gnip_1des_1url = 0
        self.stream_1des_1url = 0
        self.seed_1des_1url = 0
        self.seed_score_1des_1url = 0
        self.seed_sentifi_1des_1url = 0
        self.mention_1des_1url = 0
        self.total_1des_1url = 0

        self.gnip_1des_0url = 0
        self.stream_1des_0url = 0
        self.seed_1des_0url = 0
        self.seed_score_1des_0url = 0
        self.seed_sentifi_1des_0url = 0
        self.mention_1des_0url = 0
        self.total_1des_0url = 0

        self.gnip_0des_1url = 0
        self.stream_0des_1url = 0
        self.seed_0des_1url = 0
        self.seed_score_0des_1url = 0
        self.seed_sentifi_0des_1url = 0
        self.mention_0des_1url = 0
        self.total_0des_1url = 0

        self.gnip_0des_0url = 0
        self.stream_0des_0url = 0
        self.seed_0des_0url = 0
        self.seed_score_0des_0url = 0
        self.seed_sentifi_0des_0url = 0
        self.mention_0des_0url = 0
        self.total_0des_0url = 0

        # table 2
        self.gnip_0des_gt200 = 0
        self.stream_0des_gt200 = 0
        self.seed_0des_gt200 = 0
        self.seed_score_0des_gt200 = 0
        self.seed_sentifi_0des_gt200 = 0
        self.mention_0des_gt200 = 0
        self.remain_0des_gt200 = 0
        self.total_0des_gt200 = 0

        self.gnip_0des_gte100_lt200 = 0
        self.stream_0des_gte100_lt200 = 0
        self.seed_0des_gte100_lt200 = 0
        self.seed_score_0des_gte100_lt200 = 0
        self.seed_sentifi_0des_gte100_lt200 = 0
        self.mention_0des_gte100_lt200 = 0
        self.remain_0des_gte100_lt200 = 0
        self.total_0des_gte100_lt200 = 0

        self.gnip_0des_lt100 = 0
        self.stream_0des_lt100 = 0
        self.seed_0des_lt100 = 0
        self.seed_score_0des_lt100 = 0
        self.seed_sentifi_0des_lt100 = 0
        self.mention_0des_lt100 = 0
        self.remain_0des_lt100 = 0
        self.total_0des_lt100 = 0
        self.logger.info('Start')

    def query_es(self):
        '''
        Query elasticsearch by week
        '''
        self.query = {
                "query": {
                    "range": {
                        "timestamp": {
                            "from": my_datetime.iso_previous_days(days=7),
                            "to": my_datetime.iso_previous_days()
                        }
                    }
                },
                "aggs": {
                    "by_id": {
                        "terms": {
                            "field": "publisher.twitterID",
                            "size": 0
                        },
                        "aggs": {
                            "by_source": {
                                "terms": {
                                    "field": "source"
                                }
                            }
                        }
                    }
                }
            }

        res = self.client_ES.search(
            index="analytic_tmp",
            doc_type="relevant_document",
            body=self.query,
            request_timeout=3600,
            search_type="count"
        )
        self.aggs_by_source = []

        for pub in res['aggregations']['by_id']['buckets']:
            data = {}
            data['id'] = int(pub['key'])
            data['num_messages'] = int(pub['doc_count'])

            # data['total_score'] = 0
            for sub_pub in pub['by_source']['buckets']:
                data[sub_pub['key']] = sub_pub['doc_count']
            self.aggs_by_source.append(data)

        self.logger.info('Done query from ES')

    def copy_gnip_stream(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            DELETE FROM user_from_es
        ''')
        self.con_da0.commit()

        # create index idx_user_from_es_user_id on twitter.user_from_es using btree(user_id);
        cpy = BytesIO()
        for data in self.aggs_by_source:
            cpy.write(str(data['id']) + '\t' + str(True if data.get('gnip') else False) + '\t' + str(True if data.get('stream') else False) + '\t' + str(data['num_messages']) + '\n')

        cpy.seek(0)
        cur_da0.copy_from(cpy, 'user_from_es', columns=('user_id', 'gnip', 'stream', 'num_messages'))
        self.con_da0.commit()
        self.logger.info('Done copy gnip-stream')

    def upsert_gnip_stream(self):
        '''
        Upsert result from elasticsearch to da0
        '''
        cur_da0 = self.con_da0.cursor()

        cpy = BytesIO()
        for data in self.aggs_by_source:
            cur_da0.execute('''
                UPDATE twitter.user_from_es
                SET gnip = gnip or %s,
                    stream = stream or %s,
                    num_messages = num_messages + %s,
                    updated_at = now()
                WHERE user_id = %s
            ''', [(True if data.get('gnip') else False), (True if data.get('stream') else False), data['num_messages'], data['id']])
            if not cur_da0.rowcount:
                cpy.write(str(data['id']) + '\t' + str(True if data.get('gnip') else False) + '\t' + str(True if data.get('stream') else False) + '\t' + str(data['num_messages']) + '\n')

        cpy.seek(0)
        cur_da0.copy_from(cpy, 'user_from_es', columns=('user_id', 'gnip', 'stream', 'num_messages'))
        self.con_da0.commit()
        self.logger.info('Done upsert gnip-stream')

    def query_source(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT user_id, gnip, stream, num_messages
            FROM twitter.user_from_es
        ''')
        self.logger.info('Done query gnip-stream')

        # count in da0 find description? url? and num_tweet
        for data in cur_da0:
            cur2_da0 = self.con_da0.cursor()
            cur2_da0.execute('''
                SELECT user_id,
                    payload->>'description' != '',
                    (payload->>'url') IS NOT NULL
                FROM twitter.tw_user u
                WHERE user_id = %s
                    AND NOT EXISTS(
                        SELECT NULL
                        FROM twitter.user_tracking ut
                        WHERE ut.user_id = u.user_id
                            AND ut.status = 1
                            )
            ''', [data[0]])
            user_data = cur2_da0.fetchone()

            if not user_data:
                continue

            # table 1
            if user_data[1]:
                if user_data[2]:
                    if data[1]:
                        self.gnip_1des_1url += 1
                    elif data[2]:
                        self.stream_1des_1url += 1
                else:
                    if data[1]:
                        self.gnip_1des_0url += 1
                    elif data[2]:
                        self.stream_1des_0url += 1
            else:
                if user_data[2]:
                    if data[1]:
                        self.gnip_0des_1url += 1
                    elif data[2]:
                        self.stream_0des_1url += 1
                else:
                    if data[1]:
                        self.gnip_0des_0url += 1
                    elif data[2]:
                        self.stream_0des_0url += 1

                add_source = False
                #table 2
                #count source gnip
                if data[1]:
                    add_source = True
                    if data[3] >= 200:
                        self.gnip_0des_gt200 += 1
                    elif data[3] < 200 and data[3] >= 100:
                        self.gnip_0des_gte100_lt200 += 1
                    else:
                        self.gnip_0des_lt100 += 1
                #count source stream
                elif data[2]:
                    add_source = True
                    if data[3] >= 200:
                        self.stream_0des_gt200 += 1
                    elif data[3] < 200 and data[3] >= 100:
                        self.stream_0des_gte100_lt200 += 1
                    else:
                        self.stream_0des_lt100 += 1

                #get user source
                cur2_da0.execute('''
                    SELECT source_id
                    FROM twitter.user_source
                    WHERE user_id = %s
                ''', [data[0]])
                source_id = cur2_da0.fetchone()
                if source_id:
                    source_id = source_id[0]
                    #count source seed
                    if source_id in (4, 5):
                        add_source = True
                        if data[3] >= 200:
                            self.seed_0des_gt200 += 1
                        elif data[3] < 200 and data[3] >= 100:
                            self.seed_0des_gte100_lt200 += 1
                        else:
                            self.seed_0des_lt100 += 1
                    #count source seed by top sentifi score
                    elif source_id == 10:
                        add_source = True
                        if data[3] >= 200:
                            self.seed_score_0des_gt200 += 1
                        elif data[3] < 200 and data[3] >= 100:
                            self.seed_score_0des_gte100_lt200 += 1
                        else:
                            self.seed_score_0des_lt100 += 1
                    #count source seed by sentifi
                    elif source_id == 11:
                        add_source = True
                        if data[3] >= 200:
                            self.seed_sentifi_0des_gt200 += 1
                        elif data[3] < 200 and data[3] >= 100:
                            self.seed_sentifi_0des_gte100_lt200 += 1
                        else:
                            self.seed_sentifi_0des_lt100 += 1
                    #count source mention
                    elif source_id in (6, 8, 9):
                        add_source = True
                        if data[3] >= 200:
                            self.mention_0des_gt200 += 1
                        elif data[3] < 200 and data[3] >= 100:
                            self.mention_0des_gte100_lt200 += 1
                        else:
                            self.mention_0des_lt100 += 1

                if not add_source:
                    if data[3] >= 200:
                        self.remain_0des_gt200 += 1
                    elif data[3] < 200 and data[3] >= 100:
                        self.remain_0des_gte100_lt200 += 1
                    else:
                        self.remain_0des_lt100 += 1

        self.logger.info('Done query res-url')

    def count_da0(self):
        cur_da0 = self.con_da0.cursor()

        #count source seed by top sentifi score
        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 10
                    AND u.user_id = us.user_id
            ) AND payload->>'description' != ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.seed_score_1des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 10
                    AND u.user_id = us.user_id
            ) AND payload->>'description' != ''
                AND (payload->>'url') IS NULL
        ''')
        self.seed_score_1des_0url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 10
                    AND u.user_id = us.user_id
            ) AND payload->>'description' = ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.seed_score_0des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 10
                    AND u.user_id = us.user_id
            ) AND payload->>'description' = ''
                AND (payload->>'url') IS NULL
        ''')
        self.seed_score_0des_0url = cur_da0.fetchone()[0]

        #count source seed by sentifi
        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 11
                    AND u.user_id = us.user_id
            ) AND payload->>'description' != ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.seed_sentifi_1des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 11
                    AND u.user_id = us.user_id
            ) AND payload->>'description' != ''
                AND (payload->>'url') IS NULL
        ''')
        self.seed_sentifi_1des_0url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 11
                    AND u.user_id = us.user_id
            ) AND payload->>'description' = ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.seed_sentifi_0des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id = 11
                    AND u.user_id = us.user_id
            ) AND payload->>'description' = ''
                AND (payload->>'url') IS NULL
        ''')
        self.seed_sentifi_0des_0url = cur_da0.fetchone()[0]

        #count mention table 1
        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id in (6, 8, 9)
                    AND u.user_id = us.user_id
            ) AND payload->>'description' != ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.mention_1des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id in (6, 8, 9)
                    AND u.user_id = us.user_id
            ) AND payload->>'description' != ''
                AND (payload->>'url') IS NULL
        ''')
        self.mention_1des_0url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id in (6, 8, 9)
                    AND u.user_id = us.user_id
            ) AND payload->>'description' = ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.mention_0des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user u
            WHERE EXISTS (
                SELECT user_id
                FROM twitter.user_source AS us
                WHERE us.source_id in (6, 8, 9)
                    AND u.user_id = us.user_id
            ) AND payload->>'description' = ''
                AND (payload->>'url') IS NULL
        ''')
        self.mention_0des_0url = cur_da0.fetchone()[0]

        #count total table 1
        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user
            WHERE payload->>'description' != ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.total_1des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user
            WHERE payload->>'description' != ''
                AND (payload->>'url') IS NULL
        ''')
        self.total_1des_0url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user
            WHERE payload->>'description' = ''
                AND (payload->>'url') IS NOT NULL
        ''')
        self.total_0des_1url = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM twitter.tw_user
            WHERE payload->>'description' = ''
                AND (payload->>'url') IS NULL
        ''')
        self.total_0des_0url = cur_da0.fetchone()[0]
        self.logger.info('Done count data')

    def breakdown_statuses(self):
        cur_da0 = self.con_da0.cursor()
        cur_da0.execute('''
            SELECT ut.status, s.source_id AS id, count(*)
            FROM user_tracking ut
                JOIN user_source us ON ut.user_id = us.user_id
                JOIN source s ON s.source_id = us.source_id
            WHERE ut.updated_at >= %s
            GROUP BY ut.status, s.source_id
        ''', [my_datetime.date_previous_days(days=7)])

        value = [[0]*20 for _ in range(20)]
        for doc in cur_da0:
             value[doc[1]][doc[0]] = doc[2]

        self.gnip_breakdown = [value[1][3], value[1][9], sum(value[1][15:17])]
        self.stream_breakdown = [value[7][3], value[7][9], sum(value[7][15:17])]
        self.seed_breakdown = [value[5][3], value[5][9], sum(value[5][15:17])]
        self.mention_breakdown = [value[6][3], value[6][9], sum(value[6][15:17])]

    def close_connect(self):
        self.con_da0.close()

    def print_result(self):
        self.seed_1des_1url = self.total_1des_1url - self.gnip_1des_1url - self.stream_1des_1url - self.seed_score_1des_1url - self.seed_sentifi_1des_1url - self.mention_1des_1url
        self.seed_1des_0url = self.total_1des_0url - self.gnip_1des_0url - self.stream_1des_0url - self.seed_score_1des_0url - self.seed_sentifi_1des_0url - self.mention_1des_0url
        self.seed_0des_1url = self.total_0des_1url - self.gnip_0des_1url - self.stream_0des_1url - self.seed_score_0des_1url - self.seed_sentifi_0des_1url - self.mention_0des_1url
        self.seed_0des_0url = self.total_0des_0url - self.gnip_0des_0url - self.stream_0des_0url - self.seed_score_0des_0url - self.seed_sentifi_0des_0url - self.mention_0des_0url

        self.logger.info('table 1:')
        self.logger.info('1des_1url:  ' + str(self.gnip_1des_1url) + ' ' + str(self.stream_1des_1url) + ' ' + str(self.seed_1des_1url) + ' ' + str(self.mention_1des_1url) + ' ' + str(self.total_1des_1url))
        self.logger.info('1des_0url:  ' + str(self.gnip_1des_0url) + ' ' + str(self.stream_1des_0url) + ' ' + str(self.seed_1des_0url) + ' ' + str(self.mention_1des_0url) + ' ' + str(self.total_1des_0url))
        self.logger.info('0des_1url:  ' + str(self.gnip_0des_1url) + ' ' + str(self.stream_0des_1url) + ' ' + str(self.seed_0des_1url) + ' ' + str(self.mention_0des_1url) + ' ' + str(self.total_0des_1url))
        self.logger.info('0des_0url:  ' + str(self.gnip_0des_0url) + ' ' + str(self.stream_0des_0url) + ' ' + str(self.seed_0des_0url) + ' ' + str(self.mention_0des_0url) + ' ' + str(self.total_0des_0url))


        self.total_0des_gt200 = self.gnip_0des_gt200 + self.stream_0des_gt200 + self.seed_0des_gt200 + self.seed_score_0des_gt200 + self.seed_sentifi_0des_gt200 + self.mention_0des_gt200 + self.remain_0des_gt200
        self.total_0des_gte100_lt200 = self.gnip_0des_gte100_lt200 + self.stream_0des_gte100_lt200 + self.seed_0des_gte100_lt200 + self.seed_score_0des_gte100_lt200 + self.seed_sentifi_0des_gte100_lt200 + self.mention_0des_gte100_lt200 + self.remain_0des_gte100_lt200
        self.total_0des_lt100 = self.gnip_0des_lt100 + self.stream_0des_lt100 + self.seed_0des_lt100 + self.seed_score_0des_lt100 + self.seed_sentifi_0des_lt100 + self.mention_0des_lt100 + self.remain_0des_lt100

        self.logger.info('table 2:')
        self.logger.info('>=200:      ' + str(self.gnip_0des_gt200) + ' ' + str(self.stream_0des_gt200) + ' ' + str(self.seed_0des_gt200) + ' ' + str(self.mention_0des_gt200) + ' ' + str(self.remain_0des_gt200) + ' ' + str(self.total_0des_gt200))
        self.logger.info('>=100 <200: ' + str(self.gnip_0des_gte100_lt200) + ' ' + str(self.stream_0des_gte100_lt200) + ' ' + str(self.seed_0des_gte100_lt200) + ' ' + str(self.mention_0des_gte100_lt200) + ' ' + str(self.remain_0des_gte100_lt200) + ' ' + str(self.total_0des_gte100_lt200))
        self.logger.info('<100:       ' + str(self.gnip_0des_lt100) + ' ' + str(self.stream_0des_lt100) + ' ' + str(self.seed_0des_lt100) + ' ' + str(self.mention_0des_lt100) + ' ' + str(self.remain_0des_lt100) + ' ' + str(self.total_0des_lt100))

    def insert_db(self):
        self.print_result()
        con_dev = my_connection.dev_connection('nhat')
        cur_dev = con_dev.cursor()

        list_id = [KPIFromDb.ID_SOURCE_1DES_1URL, KPIFromDb.ID_SOURCE_1DES_0URL, KPIFromDb.ID_SOURCE_0DES_1URL, KPIFromDb.ID_SOURCE_0DES_0URL, KPIFromDb.ID_SOURCE_0DES_GT200, KPIFromDb.ID_SOURCE_0DES_GTE100_LT200, KPIFromDb.ID_SOURCE_0DES_LT100]
        list_value = [[self.gnip_1des_1url, self.stream_1des_1url, self.seed_1des_1url, self.seed_score_1des_1url, self.seed_sentifi_1des_1url, self.mention_1des_1url],
            [self.gnip_1des_0url, self.stream_1des_0url, self.seed_1des_0url, self.seed_score_1des_0url, self.seed_sentifi_1des_0url, self.mention_1des_0url],
            [self.gnip_0des_1url, self.stream_0des_1url, self.seed_0des_1url, self.seed_score_1des_1url, self.seed_sentifi_0des_1url, self.mention_0des_1url],
            [self.gnip_0des_0url, self.stream_0des_0url, self.seed_0des_0url, self.seed_score_0des_0url, self.seed_sentifi_0des_0url, self.mention_0des_0url],
            [self.gnip_0des_gt200, self.stream_0des_gt200, self.seed_0des_gt200, self.seed_score_0des_gt200, self.seed_sentifi_0des_gt200, self.mention_0des_gt200, self.remain_0des_gt200],
            [self.gnip_0des_gte100_lt200, self.stream_0des_gte100_lt200, self.seed_0des_gte100_lt200, self.seed_score_0des_gte100_lt200, self.seed_sentifi_0des_gte100_lt200, self.mention_0des_gte100_lt200, self.remain_0des_gte100_lt200],
            [self.gnip_0des_lt100, self.stream_0des_lt100, self.seed_0des_lt100, self.seed_score_0des_lt100, self.seed_sentifi_0des_lt100, self.mention_0des_lt100, self.remain_0des_lt100]]
        for i in range(len(list_id)):
            for value in list_value[i]:
                cur_dev.execute('''
                    INSERT INTO public.kpi_report(id, created_at, value)
                    VALUES(%s, now(), %s)
                ''', [list_id[i], value])
                con_dev.commit()

        list_id = [KPIFromDb.ID7_GNIP_BREAKDOWN, KPIFromDb.ID7_STREAM_BREAKDOWN, KPIFromDb.ID7_SEED_BREAKDOWN, KPIFromDb.ID7_MENTION_BREAKDOWN]
        list_value = [self.gnip_breakdown, self.stream_breakdown, self.seed_breakdown, self.mention_breakdown]
        for i in range(len(list_id)):
            for value in list_value[i]:
                cur_dev.execute('''
                    INSERT INTO public.kpi_report(id, created_at, value)
                    VALUES(%s, now(), %s)
                ''', [list_id[i], value])
                con_dev.commit()

        con_dev.close()


if __name__ == '__main__':
    weekly_report = WeeklySourceReport()
    weekly_report.query_es()
    # weekly_report4.copy_gnip_stream()
    weekly_report.upsert_gnip_stream()
    weekly_report.query_source()
    weekly_report.count_da0()
    weekly_report.breakdown_statuses()
    weekly_report.close_connect()
    weekly_report.insert_db()
