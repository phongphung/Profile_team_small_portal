__author__ = 'sunary'


import os
from twitter.twitter_services import TwitterService
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from utils import my_connection


class GetScoresFollowing():
    MAX_WEEK = 10
    MAX_FOLLOWING = 4

    def __init__(self):
        self.cluster = my_connection.cassandra_cluster()
        self.session = self.cluster.connect('publisher_ranking')
        self.con_psql1 = my_connection.psql1_connection()

    def process(self, input_sns_id):
        '''
        Return score of following' input_sns_id as google chart format

        Args:
            input_sns_id (string of int): twitter id of user
        Return:
            following' scores as google chart format
        '''
        sns_id_following = self.get_following(input_sns_id)
        sns_id_following[0:0] = input_sns_id
        metric_id = 37
        score_following = []

        added_following = 0
        for sns_id in sns_id_following:
            if self.check_exist_publisher(str(sns_id)):
                score = self.get_scores(sns_id, metric_id)
                if score:
                    score_following.append({'screenname': str(sns_id), 'score': score})
                    added_following += 1
                    if added_following >= self.MAX_FOLLOWING:
                        break

        following_id = []
        for i in range(len(score_following)):
            following_id.append(score_following[i]['screenname'])

        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        following_screenname = twitter_service.get_screen_names_by_ids(following_id)
        for i in range(len(score_following)):
            score_following[i]['screenname'] = str(following_screenname[i])

        if not score_following:
            return None

        display_score = [[None]*(len(score_following) + 1) for _ in range(self.MAX_WEEK + 1)]
        for i in range(self.MAX_WEEK + 1):
            if i == 0:
                display_score[i][0] = 'screenname'
            elif i == self.MAX_WEEK:
                display_score[i][0] = 'this week'
            else:
                display_score[i][0] = '-%s week' % (self.MAX_WEEK - i)

            if i == 0:
                for j in range(1, len(score_following) + 1):
                    display_score[i][j] = score_following[j - 1]['screenname']
            else:
                for j in range(1, len(score_following) + 1):
                    if self.MAX_WEEK - i - 1 < len(score_following[j - 1]['score']):
                        display_score[i][j] = score_following[j - 1]['score'][self.MAX_WEEK - i - 1]
                    else:
                        display_score[i][j] = 0

        return display_score

    def get_following(self, sns_id):
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        return twitter_service.get_following(user_id= sns_id)

    def check_exist_publisher(self, sns_id):
        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT True
            FROM mg_publisher_sns_clean
            WHERE sns_id = %s
                AND sns_name = 'tw'
        ''', [sns_id])

        return cur_psql1.rowcount

    def get_scores(self, sns_id, metric_id = 37):
        rows = self.session.execute('''
            SELECT stat_value
            FROM weekly_sns_stats
            WHERE sns_name = 'tw'
                AND sns_id = '%s'
                AND metric_id = %s
        ''' % (sns_id, metric_id))
        return [score.stat_value for score in rows]

    def get_scores_prepare(self, sns_id, metric_id = 37):
        prepare_statement = self.session.prepare('''
            SELECT stat_value
            FROM weekly_sns_stats
            WHERE sns_name = 'tw'
                AND sns_id = ?
                AND metric_id = ?
        ''')
        rows = self.session.execute(prepare_statement, (sns_id, metric_id))
        return [score.stat_value for score in rows]

    def get_scores_batch(self, sns_id, metric_id = 37):
        '''
        only UPDATE, INSERT and DELETE statements are allowed

        Args:
            sns_id (string of int): twitter id of user
            metric_id (int): twitter id metric
        Return:
            score value of (sns_id, metric_id)
        '''
        prepare_statement = self.session.prepare('''
            SELECT stat_value
            FROM weekly_sns_stats
            WHERE sns_name = 'tw'
                AND sns_id = ?
                AND metric_id = ?
        ''')
        batch_statement = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        batch_statement.add(prepare_statement, (sns_id, metric_id))
        rows = self.session.execute(batch_statement)
        return [score.stat_value for score in rows]

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()
if __name__ == '__main__':
    get_score = GetScoresFollowing()
    print get_score.get_scores('1533387092')
    print get_score.get_scores_prepare('1533387092')
    get_score.close_connection()