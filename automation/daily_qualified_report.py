__author__ = 'sunary'


from utils import  my_connection
from automation.kpi_from_db import KPIFromDb


class DailyQualifiedReport():

    def __init__(self):
        self.con_da0 = my_connection.da0_connection()

    def process(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM tw_user
        ''')
        self.total_candidates = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM user_tracking
            WHERE status NOT IN (1,5,6)
        ''')
        self.total_processed = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM user_tracking
            WHERE is_exported = True
        ''')
        self.total_exported = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM user_tracking
            WHERE is_classified = True
        ''')
        self.total_classified = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM user_tracking
            WHERE is_qualified = True
        ''')
        self.total_qualified = cur_da0.fetchone()[0]

        cur_da0.execute('''
            SELECT COUNT(*)
            FROM user_tracking
            WHERE status = 3
        ''')
        self.total_disqualified = cur_da0.fetchone()[0]

        self.qualified_method = []
        for status in KPIFromDb.QUALIFIED_STATUSES[::-1]:
            cur_da0.execute('''
                SELECT COUNT(*)
                FROM user_tracking
                WHERE status = %s
            ''', [status['status']])
            self.qualified_method.append(cur_da0.fetchone()[0])

    def breakdown_statuses(self):
        cur_da0 = self.con_da0.cursor()
        cur_da0.execute('''
            SELECT ut.status, s.source_id AS id, count(*)
            FROM user_tracking ut
                JOIN user_source us ON ut.user_id = us.user_id
                JOIN source s ON s.source_id = us.source_id
            GROUP BY ut.status, s.source_id
        ''')

        value = [[0]*20 for _ in range(20)]
        for doc in cur_da0:
             value[doc[1]][doc[0]] = doc[2]

        self.gnip_breakdown = [value[1][3], value[1][4], value[1][9], sum(value[1][15:17]), sum(value[1][8:17])]
        self.stream_breakdown = [value[7][3], value[7][4], value[7][9], sum(value[7][15:17]), sum(value[7][8:17])]
        self.seed_breakdown = [value[5][3], value[5][4], value[5][9], sum(value[5][15:17]), sum(value[5][8:17])]
        self.mention_breakdown = [value[6][3], value[6][4], value[6][9], sum(value[6][15:17]), sum(value[6][8:17])]

        self.con_da0.close()

    def insert_db(self):
        con_dev = my_connection.dev_connection('nhat')

        list_id = [KPIFromDb.ID_TOTAL_CANDIDATES, KPIFromDb.ID_TOTAL_PROCESSED, KPIFromDb.ID_TOTAL_EXPORTED, KPIFromDb.ID_TOTAL_CLASSIFIED, KPIFromDb.ID_TOTAL_QUALIFIED, KPIFromDb.ID_TOTAL_DISQUALIFIED]
        list_value = [self.total_candidates, self.total_processed, self.total_exported, self.total_classified, self.total_qualified, self.total_disqualified]

        cur_dev = con_dev.cursor()
        for i in range(len(list_id)):
            cur_dev.execute('''
                INSERT INTO public.kpi_report(id, created_at, value)
                VALUES(%s, now(), %s)
            ''', [list_id[i], list_value[i]])
            con_dev.commit()

        list_id = KPIFromDb.ID_QUALIFIED_STATUSES
        for method in self.qualified_method:
            cur_dev.execute('''
                INSERT INTO public.kpi_report(id, created_at, value)
                VALUES(%s, now(), %s)
            ''', [list_id, method])
            con_dev.commit()

        list_id = [KPIFromDb.ID1_GNIP_BREAKDOWN, KPIFromDb.ID1_STREAM_BREAKDOWN, KPIFromDb.ID1_SEED_BREAKDOWN, KPIFromDb.ID1_MENTION_BREAKDOWN]
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
    daily_qualified_report = DailyQualifiedReport()
    daily_qualified_report.process()
    daily_qualified_report.breakdown_statuses()
    daily_qualified_report.insert_db()
