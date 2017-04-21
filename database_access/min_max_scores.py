__author__ = 'sunary'


from utils import my_helper, my_datetime, my_connection
import datetime


class MinMaxScore():

    def __init__(self):
        self.cluster = my_connection.cassandra_cluster()
        self.session = self.cluster.connect('publisher_ranking')
        self.logger = my_helper.init_logger(self.__class__.__name__)

    def get(self):
        self.min_max_stat = [[999999999, -999999999] for _ in range(55)]
        self.get_by_metric()
        # self.full_scan()
        self.close_connection()

        return self.min_max_stat

    def get_by_metric(self):
        '''
        get min_max stat by metric_id last 8 weeks (from 25th week)
        '''
        stat_date = [my_datetime.date_range_of_weekth(25 + i, 2015)[0] for i in range(8)]
        metric_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 51, 52, 53, 54]
        for metric in metric_id:
            min_stat, max_stat = self.min_max_stat[0]
            for date in stat_date:
                prepare_statement = self.session.prepare('''
                    SELECT stat_value
                    FROM weekly_metric
                    WHERE sns_name = 'tw'
                        AND metric_id = ?
                        AND stat_date = ?
                ''')
                rows = self.session.execute(prepare_statement, (metric, date))

                for stat in rows:
                    if stat.stat_value < min_stat:
                        min_stat = stat.stat_value
                    if stat.stat_value > max_stat:
                        max_stat = stat.stat_value

            self.logger.info('metric %s: %s,%s'% (metric, min_stat, max_stat))
            self.min_max_stat[metric] = [min_stat, max_stat]

    def full_scan(self):
        '''
        get min_max stat full scan cassandra using token
        '''
        rows = self.session.execute('''
            SELECT TOKEN(sns_name, sns_id, metric_id)
            FROM weekly_sns_stats
            LIMIT 1
        ''')

        last_token = rows[0].token_sns_name__sns_id__metric_id
        scan_from = datetime.datetime(2015, 06, 15)
        empty_iterator = False
        while not empty_iterator:
            empty_iterator = True
            prepare_statement = self.session.prepare('''
                SELECT TOKEN(sns_name, sns_id, metric_id), metric_id, stat_value, stat_date
                FROM weekly_sns_stats
                WHERE TOKEN(sns_name, sns_id, metric_id) > ?
                    AND stat_date >= ?
                LIMIT 50000
                ALLOW FILTERING
            ''')
            rows = self.session.execute(prepare_statement, (last_token, scan_from))

            for score in rows:
                if score.stat_value < self.min_max_stat[score.metric_id][0]:
                    self.min_max_stat[score.metric_id][0] = score.stat_value
                if score.stat_value > self.min_max_stat[score.metric_id][1]:
                    self.min_max_stat[score.metric_id][1] = score.stat_value

                last_token = score.token_sns_name__sns_id__metric_id
                empty_iterator = False

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()


if __name__ == '__main__':
    get_main_max_score = MinMaxScore()
    print get_main_max_score.get()