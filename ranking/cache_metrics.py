__author__ = 'sunary'


from utils import my_connection, my_datetime
from redis import StrictRedis
import sys
from argparse import ArgumentParser


class CacheMetrics():
    REDIS_TEMP_TTL = 2 * 7 * 24 * 3600
    def __init__(self):
        self.cluster = my_connection.cassandra_cluster()
        self.session = self.cluster.connect('publisher_ranking')
        self.redis_cluster = StrictRedis(host='ranking.ssh.sentifi.com', port=6379)

    def process(self, metrics, week, year):
        for metric in metrics:
            self.read_cassandra(metric, week, year)

    def read_cassandra(self, metric, week, year):
        date = my_datetime.date_range_of_weekth(week, year)[0]

        prepare_statement = self.session.prepare('''
            SELECT stat_value,
                sns_id
            FROM weekly_metric
            WHERE sns_name = 'tw'
                AND metric_id = ?
                AND stat_date = ?
        ''')
        rows = self.session.execute(prepare_statement, (metric, date))
        for stat in rows:
            self.redis_cluster.hmset('sns:%s-%s:signal:%s-%s' % ('tw', stat.sns_id, year, week), {metric: stat.stat_value})
            self.redis_cluster.expire('sns:%s-%s:signal:%s-%s' % ('tw', stat.sns_id, year, week), self.REDIS_TEMP_TTL)

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()

def main(options):
    parser = ArgumentParser(prog='Cache Metrics')
    parser.add_argument('-w', '--week', dest='week', type=int,
                        help='The week number to retrieve and scale data')
    parser.add_argument('-y', '--year', dest='year', type=int,
                        help='The year number to retrieve and scale data')
    parser.add_argument('-m', '--metrics', dest='metrics', type=int, nargs='+',
                        help='The list of metric ids to cache')
    args = parser.parse_args()

    cache_metrics = CacheMetrics()
    cache_metrics.process(args.metrics, args.week, args.year)
    cache_metrics.close_connection()

if __name__ == '__main__':
    main(sys.argv[1:])