__author__ = 'sunary'

# chau.tran
# Change to FlaskView, move to a kpi class environment
__version__ = '1.1'

from datetime import datetime, timedelta
from automation.kpi_from_db import KPIFromDb
from flask import Flask, render_template, current_app
from flask.ext.cache import Cache
from flask.ext.classy import FlaskView, route
from utils import my_connection

# Set up flask and cache
flask_app = Flask(__name__)
flask_app.config['UPLOAD_FOLDER'] = 'upload/'

cache = Cache(flask_app, config = {'CACHE_TYPE' : 'simple'})
cache.init_app(flask_app)


class Kpi(FlaskView):
    """
        Class Kpi to handle KPI project
        Args:
            A class of Flask framework
    """
    route_base = '/'

    def __init__(self):
        """
            Initialize index page
        """
        self.index()
    
#    @route('/index.html', endpoint='index'), not recommended to do this
    @route('/')
    @cache.cached(timeout=3600)
    def index(self):
        """
            Index page of KPI
        """
        return render_template('index.html')

    @route('/daily_report')
    @cache.cached(timeout=3600)
    def daily_report(self):
        """
            Display daily reports 
        """
        from timeit import default_timer as timer

        kpi_db = KPIFromDb()
# Read daily messages report
        start = timer()
        daily_messages_report = kpi_db.read_daily_messages_report()
        end = timer()
        print "Time_read_daily_msg_report = {}".format(end - start)

        start = timer()
        daily_qualified = kpi_db.read_daily_qualified_report()
        end = timer()
        print "Time_read_daily_qualified_report = {}".format(end - start)
        
        start = timer()
        method_table, method_chart = kpi_db.read_method_qualified_report()
        end = timer()
        print "Time_read_method_qualified_report = {}".format(end - start)

        start = timer()
        cumulative_breakdown = kpi_db.read_cumulative_breakdown()
        end = timer()
        print "Time_read_cumulative_breakdown = {}".format(end - start)

        start = timer()
        profile_change = kpi_db.read_profile_change()
        end = timer()
        print "Time_read_profile_change = {}".format(end - start)
        kpi_db.close_connection()

        message_processed = []
        date_chart = []
        for i in range(7):
            message_processed.append(
                daily_messages_report[0][i] + daily_messages_report[1][i + 1] - daily_messages_report[1][i])
            date_chart.append((datetime.today() - timedelta(days=i + 1)).strftime('%d/%m'))

        return render_template('daily_report.html',
                               message_orange_flow=daily_messages_report[0],
                               message_in_queue=daily_messages_report[1],
                               message_processed=message_processed,
                               message_total_candidates=daily_messages_report[2],

                               publisher_total_candidates=daily_qualified[0],
                               publisher_total_processed=daily_qualified[1],
                               publisher_total_exported=daily_qualified[2],
                               publisher_total_classified=daily_qualified[3],
                               publisher_total_qualified=daily_qualified[4],
                               publisher_total_disqualified=daily_qualified[5],

                               qualifying_method_table=method_table,
                               qualifying_method_chart=method_chart,

                               gnip_breakdown=cumulative_breakdown[0],
                               stream_breakdown=cumulative_breakdown[1],
                               seed_breakdown=cumulative_breakdown[2],
                               mention_breakdown=cumulative_breakdown[3],

                               profile_change_des=profile_change[0],
                               profile_change_avatar=profile_change[1],
                               profile_change_loc=profile_change[2],
                               profile_change_name=profile_change[3],
                               profile_change_url=profile_change[4],
                               profile_change_any=profile_change[5],

                               date=(datetime.today() - timedelta(days=1)).strftime('%a, %d %b %Y'),
                               date_chart=date_chart
                               )



    def query_pub_source(self, start, end):
        from psycopg2.extras import RealDictCursor

        criterias = {
            '1-1desc_1url': """
            (payload->>'description') IS NOT NULL AND payload->>'description' != '' AND
            (payload->>'url') IS NOT NULL AND payload->>'url' != ''
            """,
            '2-1desc_0url': """
            (payload->>'description') IS NOT NULL AND payload->>'description' != '' AND
            ((payload->>'url') IS NULL OR payload->>'url' = '')
            """,
            '3-0desc_1url': """
            ((payload->>'description') IS NULL OR payload->>'description' = '') AND
            (payload->>'url') IS NOT NULL AND payload->>'url' != ''
            """,
            '4-0desc_0url': """
            ((payload->>'description') IS NULL OR payload->>'description' = '') AND
            ((payload->>'url') IS NULL OR payload->>'url' = '')
            """
        }

        sources = {
            'mention': [6, 8, 9],
            'sentifi_network_score': [10],
            'sentifi_network': [11],
            'on demand seed': [5],
            'stream': [7],
            'gnip': [1]
        }

        query_with_source = '''
            SELECT  us.source_id as source, COUNT(*) as count_
            FROM    twitter.user_source AS us
            WHERE   EXISTS (
                        SELECT  user_id
                        FROM    twitter.tw_user u
                        WHERE   u.user_id = us.user_id AND
                                u.created_at>=%s AND u.created_at<%s AND {criteria})
            GROUP BY us.source_id;
        '''

        query_total = '''
        SELECT  count(user_id) as count_
        FROM    twitter.tw_user u
        WHERE   u.created_at>=%s AND u.created_at<%s AND
                NOT EXISTS (
                    SELECT us.user_id
                    FROM twitter.user_source AS us
                    WHERE u.user_id = us.user_id
                ) AND {criteria};
        '''
        conn = my_connection.da0_connection(database='da0', user='dbw')
        data = {
            '1-1desc_1url': {},
            '2-1desc_0url': {},
            '3-0desc_1url': {},
            '4-0desc_0url': {}
        }
        result = {
            '1-1desc_1url': {},
            '2-1desc_0url': {},
            '3-0desc_1url': {},
            '4-0desc_0url': {},
            'total': {
                'gnip': 0,
                'mention': 0,
                'on demand seed': 0,
                'sentifi_network': 0,
                'sentifi_network_score': 0,
                'stream': 0,
                'total': 0
            }
        }
        for name, criteria in criterias.iteritems():
            print name
#            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            query = cursor.mogrify(query_with_source.format(criteria=criteria), (start, end))
            print query
            cursor.execute(query)
            rows_count = cursor.rowcount
            print rows_count, cursor.fetchall()
            
            conn.commit()
            for row in cursor:
                data[name][row['source']] = row['count_']

            query = cursor.mogrify(query_total.format(criteria=criteria), (start, end))
            print query
            cursor.execute(query)
            result[name]['no_source'] = cursor.fetchone()['count_']
            result[name]['total'] = result[name]['no_source'] + sum(data[name].values())
            result['total']['total'] += result[name]['total']

        for source_name, source_ids in sources.iteritems():
            print source_name
            for name, count_data in data.iteritems():
                temp_sum = 0
                for source_id in source_ids:
                    temp_sum += count_data.get(source_id, 0)
                result[name][source_name] = temp_sum
                result['total'][source_name] += result[name][source_name]

        return result



    def query_es_count_num_msg(self, end):
        from elasticsearch import Elasticsearch

        client_ES = Elasticsearch(
            hosts=['es0.ssh.sentifi.com',
                   'es4.ssh.sentifi.com',
                   'es5.ssh.sentifi.com',
                   'es8.ssh.sentifi.com',
                   'es9.ssh.sentifi.com'],
            retry_on_timeout=True,
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniff_timeout=3600,
            timeout=3600,
        )
        query = {
            "query": {
                "filtered": {
                    "filter": {
                        "range": {
                            "published_at": {
                                "to": end
                            }
                        }
                    }
                }
            },
            "aggs": {
                "by_id": {
                    "terms": {
                        "field": "published_by.sns_id",
                        "size": 0
                    }
                }
            }
        }

        res = client_ES.search(index="analytic_tmp3", doc_type="relevant_document", body=query,
                               request_timeout=3600, search_type="count")

        data = {
            'top': set(), 'mid': set(), 'bot': set()
        }

        for pub in res['aggregations']['by_id']['buckets']:
            num = pub['doc_count']
            if num >= 200:
                data['top'].add(int(pub['key']))
            elif num >= 100:
                data['mid'].add(int(pub['key']))
            else:
                data['bot'].add(int(pub['key']))

        return data





    def query_candidates_without_description(self):
        from psycopg2.extras import RealDictCursor

        query_candidates = '''
            SELECT  user_id
            FROM    twitter.tw_user u
            WHERE   NOT EXISTS (
                            SELECT ut.user_id
                            FROM twitter.user_tracking ut
                            WHERE ut.user_id = u.user_id AND ut.status = 1
                            )
                    AND NULLIF(payload->>'description', '') IS NULL
                    AND user_id > %s
            ORDER BY user_id
            LIMIT %s;
        '''
        conn = my_connection.da0_connection(database='da0', user='dbw')
        from_object_id = 0
        result = set()
        BATCH_SIZE = 5000

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = cursor.mogrify(query_candidates, (from_object_id, BATCH_SIZE))
            cursor.execute(query)
            conn.commit()
            counter = 0
            while cursor and cursor.rowcount > 0:
                print counter
                for index, row in enumerate(cursor):
                    user_id = row['user_id']
                    result.add(user_id)
                    from_object_id = user_id
                query = cursor.mogrify(query_candidates, (from_object_id, BATCH_SIZE))
                cursor.execute(query)
                conn.commit()
                counter += 1
                if counter % 15 == 0:
                    print 'New Batch #', counter, 'processed:', counter * BATCH_SIZE
        return result


    def query_candidates_tweets_breakdown(self, data_cur, data_prev):
        from psycopg2.extras import RealDictCursor
        from collections import defaultdict

        BATCH_SIZE = 120000
        query_source = '''
            SELECT      us.user_id AS user, us.source_id AS source
            FROM        twitter.user_source AS us
            WHERE   EXISTS (
                        SELECT  user_id
                        FROM    twitter.tw_user u
                        WHERE   u.user_id = us.user_id)
            WHERE       us.user_id > %s
            ORDER BY    us.user_id
            LIMIT       %s;
        '''

        conn = my_connection.da0_connection(database='da0', user='dbw')
        result_cur = {
            'top': defaultdict(int),
            'mid': defaultdict(int),
            'bot': defaultdict(int)
        }
        result_prev = {
            'top': defaultdict(int),
            'mid': defaultdict(int),
            'bot': defaultdict(int)
        }
        from_object_id = 0
        candidates = query_candidates_without_description()
        candidates_with_source = set()

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = cursor.mogrify(query_source, (from_object_id, BATCH_SIZE))
            cursor.execute(query)
            conn.commit()
            counter = 0
            while cursor and cursor.rowcount > 0:
                for index, row in enumerate(cursor):
                    user_id = int(row['user'])
                    candidates_with_source.add(user_id)
                    if user_id not in candidates:
                        continue
                    # print user_id
                    from_object_id = user_id
                    for num_range in result_cur.keys():
                        if user_id in data_cur[num_range]:
                            result_cur[num_range][row['source']] += 1
                            # data_cur[num_range].remove(user_id)
                    for num_range in result_prev.keys():
                        if user_id in data_prev[num_range]:
                            result_prev[num_range][row['source']] += 1
                            # data_prev[num_range].remove(user_id)
                query = cursor.mogrify(query_source, (from_object_id, BATCH_SIZE))
                cursor.execute(query)
                conn.commit()
                counter += 1
                if counter % 35 == 0:
                    print 'New Batch #', counter, 'processed:', counter * BATCH_SIZE
        for user_id in candidates.difference(candidates_with_source):
            for num_range in result_cur.keys():
                if user_id in data_cur[num_range]:
                    result_cur[num_range]['other'] += 1
            for num_range in result_prev.keys():
                if user_id in data_prev[num_range]:
                    result_prev[num_range]['other'] += 1
        return result_cur, result_prev


    @route('/weekly_report')
    @cache.cached(timeout=3600 * 6)
    def weekly_report(self):
        import pandas as pd

        names = {
            'gnip': 0,
            'stream': 1,
            'on demand seed': 2,
            'sentifi_network_score': 3,
            'sentifi_network': 4,
            'mention': 5,
            'no_source': 7,
            'total': 6
        }

        kpi_db = KPIFromDb()
        weekly_source = kpi_db.read_weekly_source_report()
        weekly_publisher = kpi_db.read_weekly_publisher_report()
        weekly_breakdown_statuses = kpi_db.read_weekly_breakdown_statuses()
        kpi_db.close_connection()

        source_total = []
#        print "length_weekly_source = {}\tweekly_source = {}".format(len(weekly_source[0]), weekly_source[0])
        for i in range(len(weekly_source[0])):
            source_total.append(0)
#            print "source_total = {}".format(source_total)
            for j in range(4):
                source_total[-1] += weekly_source[j][i]
#                print "weekly_source{}{} = {}".format(j, i, weekly_source[j][i])

        tweet_total = []
        for i in xrange(len(weekly_source[4])):
            tweet_total.append(0)
#            print "tweet_total = {}".format(tweet_total)
            for j in xrange(4,7):
                tweet_total[-1] += weekly_source[j][i]
#                print "weekly_source_tweet{}{} = {}".format(j, i, weekly_source[j][i])


        end = datetime.now()
        start = end - timedelta(days=7)
        result_cur = self.query_pub_source(start, end)
        # end = start - timedelta(microseconds=1)
        # start = end - timedelta(days=7)
        # result_prev = query_pub_source(start, end)

        diff = {
            '1-1desc_1url': {},
            '2-1desc_0url': {},
            '3-0desc_1url': {},
            '4-0desc_0url': {},
            'total': {}
#            '5_0des_gt200': {},
#            '6_0des_gte100_lt200': {},
#            '7_0des_lt100': {},
#            'tweet_total': {}
        }

        # subtract current with previous data
        # for (cur_key, cur_count), (prev_key, prev_count) in zip(result_cur.items(), result_prev.items()):
        #     for (cur_k, cur_v), (prev_k, prev_v) in zip(cur_count.items(), prev_count.items()):
        #         diff[cur_key][cur_k] = cur_v - prev_v
        df = pd.DataFrame(data=result_cur).T
        print df, names

        return render_template('weekly_report.html',
                               len_source_des_url=7,
                               source_1des_1url=weekly_source[0],
                               source_1des_0url=weekly_source[1],
                               source_0des_1url=weekly_source[2],
                               source_0des_0url=weekly_source[3],
                               source_total=source_total,
                               len_source_0des=8,
                               source_0des_gt200=weekly_source[4],
                               source_0des_gte100_lt200=weekly_source[5],
                               source_0des_lt100=weekly_source[6],
                               tweet_total=tweet_total, 
                               twitter_broken_avatar=weekly_publisher[0],
                               twitter_missing_avatar=weekly_publisher[1],
                               twitter_missing_iso=weekly_publisher[2],
                               twitter_has_many_itemkey=weekly_publisher[3],
                               twitter_has_many_publisher=weekly_publisher[4],
                               twitter_without_itemkey=weekly_publisher[5],

                               gnip_breakdown=weekly_breakdown_statuses[0],
                               stream_breakdown=weekly_breakdown_statuses[1],
                               seed_breakdown=weekly_breakdown_statuses[2],
                               mention_breakdown=weekly_breakdown_statuses[3],

                               week=datetime.now().isocalendar()[1],
                               year=datetime.now().isocalendar()[0],

                               diff=df.rename_axis(names, axis=1))


    def query_roles(self):
        from psycopg2.extras import RealDictCursor

        query = """
            SELECT role_name, role_id
            FROM role
        """
        conn = my_connection.ellytran_connection(database='rip_slave1', user='dbr')
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        conn.commit()
        if cur.rowcount > 0:
            return {row['role_id']: row['role_name'] for row in cur}
        conn.close()


    def query_categories(self):
        from psycopg2.extras import RealDictCursor

        query = """
            SELECT category_name, category_id
            FROM category
        """
#        conn = get_conn()
        conn = my_connection.ellytran_connection(database='rip_slave1', user='dbr')
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        conn.commit()
        if cur.rowcount > 0:
            return {row['category_id']: row['category_name'] for row in cur}
        conn.close()


    def query_all_pubs(self):
        from psycopg2.extras import RealDictCursor

        query = """
            SELECT country_code as country, count(*) as count_pub
            FROM sns_account JOIN (
                    SELECT  sns_name,
                            sns_id
                    FROM    sns_category JOIN category USING (category_id)
                    WHERE   category_id_path@>ARRAY[1]) as b
                USING (sns_name, sns_id)
            GROUP BY country_code
        """

        conn = my_connection.ellytran_connection(database='rip_slave1', user='dbr')
        result = {'All': {'All countries': 0}}
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        conn.commit()
        if cur.rowcount > 0:
            for row in cur:
                country = row['country']
                country = country if country and len(country.strip()) == 2 else 'Unknown'
                result['All'][country] = row['count_pub']
                result['All']['All countries'] += row['count_pub']
        conn.close()
        return result


    def query_publishers_roles(self):
        from psycopg2.extras import RealDictCursor
        import pandas as pd

        query = """
            SELECT  role_name,
                    role_id_path,
                    rc.country,
                    rc.count_pub
            FROM role JOIN (SELECT r.role_id, a.country_code as country, count(*) as count_pub
                            FROM sns_role r JOIN sns_account a USING (sns_name, sns_id)
                            GROUP BY r.role_id, a.country_code) as rc USING(role_id)
        """
#        conn = get_conn()
        conn = my_connection.ellytran_connection(database='rip_slave1', user='dbr')
        roles = self.query_roles()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        conn.commit()
        if cur.rowcount > 0:
            data2 = {'All cat': {'All countries': 0, 'name': 'All categories'}}
            all_pubs_count = self.query_all_pubs()
            data2['All'] = all_pubs_count['All']
            for row in cur:
                country = row['country']
                country = country if country and len(country.strip()) == 2 else 'Unknown'
                id_path = tuple(row['role_id_path'])
                if id_path not in data2:
                    data2[id_path] = {'All countries': 0}
                    data2[id_path]['name'] = row['role_name']
                data2[id_path][country] = row['count_pub']
                data2[id_path]['All countries'] += row['count_pub']
                if country not in data2['All cat']:
                    data2['All cat'][country] = 0
                data2['All cat'][country] += row['count_pub']
                data2['All cat']['All countries'] += row['count_pub']
            df = pd.DataFrame.from_dict(data2, orient='index')
            df['role_path'] = df.index
            df['role_names'] = df.role_path.apply(
                lambda x: ','.join([roles.get(each, '') for each in x]) if x != 'All cat' else 'All occupations and functional roles')
        conn.close()
        return df.sort('role_names').fillna(0)


    def query_publishers(self):
        from psycopg2.extras import RealDictCursor
        import pandas as pd

        query = """
            SELECT  category_name,
                    category_id_path,
                    rc.country,
                    rc.count_pub
            FROM category JOIN (SELECT r.category_id, a.country_code as country, count(*) as count_pub
                            FROM sns_category r JOIN sns_account a USING (sns_name, sns_id)
                            GROUP BY r.category_id, a.country_code) as rc USING(category_id)
        """
#        conn = get_conn()
        conn = my_connection.ellytran_connection(database='rip_slave1', user='dbr')
        categories = self.query_categories()
        cur  = conn.cursor(cursor_factory=RealDictCursor) 
        cur.execute(query)
        conn.commit()
        if cur.rowcount > 0:
            data2 = {
                'All cat': {
                    'All countries': 0,
                    'name': 'All categories'
                },
                'All persons': {
                    'All countries': 0,
                    'name': 'All persons'
                },
                'All organizations': {
                    'All countries': 0,
                    'name': 'All organizations'
                }
            }
            # all_pubs_count = query_all_pubs()
            # data2['All'] = all_pubs_count['All']
            for row in cur:
                country = row['country']
                country = country if country and len(country.strip()) == 2 else 'Unknown'
                id_path = tuple(row['category_id_path'])
                if id_path not in data2:
                    data2[id_path] = {'All countries': 0}
                    data2[id_path]['name'] = row['category_name']
                data2[id_path][country] = row['count_pub']
                data2[id_path]['All countries'] += row['count_pub']
                if country not in data2['All cat']:
                    data2['All cat'][country] = 0
                data2['All cat'][country] += row['count_pub']
                data2['All cat']['All countries'] += row['count_pub']
                if 1 in id_path:
                    if country not in data2['All persons']:
                        data2['All persons'][country] = 0
                    data2['All persons'][country] += row['count_pub']
                    data2['All persons']['All countries'] += row['count_pub']
                else:
                    if country not in data2['All organizations']:
                        data2['All organizations'][country] = 0
                    data2['All organizations'][country] += row['count_pub']
                    data2['All organizations']['All countries'] += row['count_pub']
            df = pd.DataFrame.from_dict(data2, orient='index')
            df['category_path'] = df.index
            df['category_names'] = df.category_path.apply(
                lambda x: '->'.join([categories.get(each, '') for each in x])
                if x not in ['All cat', 'All persons', 'All organizations'] else x)
        conn.close()
        return df.sort('category_names').fillna(0)


    @route('/publisher_classified')
    @cache.cached(timeout=3600 * 6)
    def publisher_classified(self):
        columns = ['role_names', 'AT', 'CH', 'DE', 'GB', 'HK', 'SG', 'CN', 'IN', 'US', 'Unknown', 'All countries']
        names = {'AT': 'Austria',
                 'CH': 'Switzerland',
                 'DE': 'Germany',
                 'GB': 'UK',
                 'HK': 'Hongkong',
                 'SG': 'Singapore',
                 'CN': 'China',
                 'IN': 'India',
                 'US': 'US',
                 'Unknown': 'Unknown',
                 'All countries': 'All countries'}
        # kpi_db = KPIFromDb()
        # publisher_qualified, categories = kpi_db.read_publisher_qualified_isocode()
        # kpi_db.close_connection()
        df = self.query_publishers_roles()
        df['role_names'][0] = 'All persons'
        return render_template('publisher_classified.html',
                               df=df[columns],
                               names=names,
                               week=datetime.now().isocalendar()[1],
                               year=datetime.now().isocalendar()[0])


    @route('/publisher_category')
    @cache.cached(timeout=3600 * 6)
    def publisher_category(self):
        columns = ['category_names', 'AT', 'CH', 'DE', 'GB', 'HK', 'SG', 'CN', 'IN', 'US', 'Unknown', 'All countries']
        names = {'AT': 'Austria',
                 'CH': 'Switzerland',
                 'DE': 'Germany',
                 'GB': 'UK',
                 'HK': 'Hongkong',
                 'SG': 'Singapore',
                 'CN': 'China',
                 'IN': 'India',
                 'US': 'US',
                 'Unknown': 'Unknown',
                 'All countries': 'All countries'}
        # kpi_db = KPIFromDb()
        # publisher_qualified, categories = kpi_db.read_publisher_qualified_isocode()
        # kpi_db.close_connection()
        df = self.query_publishers()
        # df['role_names'][0] = 'All'
        return render_template('publisher_category.html',
                               df=df[columns],
                               names=names,
                               week=datetime.now().isocalendar()[1],
                               year=datetime.now().isocalendar()[0])


    @route('/publisher_snapshot')
    @cache.cached(timeout=3600)
    def publisher_snapshot(self):
        kpi_db = KPIFromDb()
        publisher_snapshot, categories = kpi_db.read_publisher_snapshot()
        kpi_db.close_connection()

        return render_template('publisher_snapshot.html',
                               publisher_snapshot=publisher_snapshot,
                               categories=categories,
                               week=datetime.now().isocalendar()[1],
                               year=datetime.now().isocalendar()[0])


if __name__ == '__main__':        
    with flask_app.test_request_context():
        test_kpi = Kpi()
        test_kpi.daily_report()
        test_kpi.weekly_report()
        test_kpi.publisher_classified()
#        test_kpi.publisher_qualified()
        test_kpi.publisher_category()
        test_kpi.publisher_snapshot()
        test_kpi.register(flask_app)
        flask_app.run(host='10.0.0.120', port = 5372, debug = True)
    cache.clear()
