__author__ = 'sunary'

# chau.tran
# Build a config file for this class
# Organize code in a more understandable way

__version__ = '1.1'

from utils import my_connection
from automation import kpi_from_db_config


class KPIFromDb(object):
    """
        Module to handle connection between KPI and database
    """

    def __init__(self):
        """
            Set up new connection to database
        """
        self.con_dev = my_connection.dev_connection(database='nhat', user='dbw') 
        self.cursor  = self.con_dev.cursor()
    
    def read_daily_messages_report(self):
        """
            Read daily reports from database
        """
        from itertools import repeat

        self.ID_TWEET_ORANGE_FLOW    = kpi_from_db_config.ID_TWEET_ORANGE_FLOW
        self.ID_PROCESSING_MESSAGES  = kpi_from_db_config.ID_PROCESSING_MESSAGES
        self.ID_CANDIDATES_PROCESSED = kpi_from_db_config.ID_CANDIDATES_PROCESSED

        list_id = [self.ID_TWEET_ORANGE_FLOW, 
                    self.ID_PROCESSING_MESSAGES, 
                    self.ID_CANDIDATES_PROCESSED]
        len_need_list = [7, 8, 2]
        list_result = [[] for i in repeat(None,len(list_id))]

        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT %s
            ''', [list_id[i], len_need_list[i]])
            rows_count = self.cursor.rowcount

            if (rows_count == len_need_list[i]): # If rows_count as expected 
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
            elif (rows_count > 0 and rows_count < len_need_list[i]):
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
                list_result[i] = list_result[i] + [0] * (len_need_list[i] - rows_count)   
            else:
                list_result[i] = [0] * len_need_list[i]

        return list_result


    def read_daily_qualified_report(self):
        """
            Read qualified daily reports from database
        """
        from itertools import repeat

        self.ID_TOTAL_CANDIDATES     = kpi_from_db_config.ID_TOTAL_CANDIDATES
        self.ID_TOTAL_PROCESSED      = kpi_from_db_config.ID_TOTAL_PROCESSED
        self.ID_TOTAL_EXPORTED       = kpi_from_db_config.ID_TOTAL_EXPORTED
        self.ID_TOTAL_CLASSIFIED     = kpi_from_db_config.ID_TOTAL_CLASSIFIED
        self.ID_TOTAL_QUALIFIED      = kpi_from_db_config.ID_TOTAL_QUALIFIED
        self.ID_TOTAL_DISQUALIFIED   = kpi_from_db_config.ID_TOTAL_DISQUALIFIED

        list_id = [self.ID_TOTAL_CANDIDATES, 
                    self.ID_TOTAL_PROCESSED, 
                    self.ID_TOTAL_EXPORTED, 
                    self.ID_TOTAL_CLASSIFIED, 
                    self.ID_TOTAL_QUALIFIED, 
                    self.ID_TOTAL_DISQUALIFIED]
        list_result = [[] for i in repeat(None,len(list_id))]

        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT 2
            ''', [list_id[i]])

            rows_count = self.cursor.rowcount
            if (rows_count == 2):
                for doc in self.cursor:
                   list_result[i].append(int(doc[0]))
            elif (rows_count == 1):
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
                list_result[i] = list_result[i] + [0]
            else:
                list_result[i] = [0] * 2 

#         print "TESTING .... {}".format(list_result)
        return list_result

    def read_method_qualified_report(self):
        """
            Read qualified method report
        """
        from itertools import repeat

        self.ID_QUALIFIED_STATUSES = kpi_from_db_config.ID_QUALIFIED_STATUSES
        self.QUALIFIED_STATUSES    = kpi_from_db_config.QUALIFIED_STATUSES

        qualifying_method_table = []
        qualifying_method_chart = []

        length_statuses = len(self.ID_QUALIFIED_STATUSES) + 2 * len(self.QUALIFIED_STATUSES)

        self.cursor.execute('''
            SELECT value
            FROM public.kpi_report
            WHERE id = %s
            ORDER BY created_at DESC
            LIMIT %s
            ''', 
            [self.ID_QUALIFIED_STATUSES, 
            2*len(self.QUALIFIED_STATUSES)])
            
        rows_count = self.cursor.rowcount
        list_result = []
        if (rows_count == length_statuses):
            for doc in self.cursor:
                list_result.append(int(doc[0]))
        elif (rows_count > 0 and rows_count < length_statuses):
            for doc in self.cursor:
                list_result.append(int(doc[0]))
            list_result = list_result + [0] * (length_statuses - rows_count)
        else:
            list_result = [0] * length_statuses


#         print "TESTING .... {}".format(list_result)
        qualifying_method_chart.append(['Method', 'count'])
        for i in range(len(self.QUALIFIED_STATUSES)):
            qualifying_method_table.append({'method_name': self.QUALIFIED_STATUSES[i]['name'], 
                'count': list_result[i], 
                'old': list_result[i + len(self.QUALIFIED_STATUSES)]})
            qualifying_method_chart.append([self.QUALIFIED_STATUSES[i]['name'], list_result[i]])

        return qualifying_method_table, qualifying_method_chart

    def read_cumulative_breakdown(self):
        """
            Read cumulative breakdown from statistics
        """
        from itertools import repeat

        self.ID1_GNIP_BREAKDOWN    = kpi_from_db_config.ID1_GNIP_BREAKDOWN
        self.ID1_STREAM_BREAKDOWN  = kpi_from_db_config.ID1_STREAM_BREAKDOWN
        self.ID1_SEED_BREAKDOWN    = kpi_from_db_config.ID1_SEED_BREAKDOWN
        self.ID1_MENTION_BREAKDOWN = kpi_from_db_config.ID1_MENTION_BREAKDOWN

        list_id = [self.ID1_GNIP_BREAKDOWN, 
                    self.ID1_STREAM_BREAKDOWN, 
                    self.ID1_SEED_BREAKDOWN, 
                    self.ID1_MENTION_BREAKDOWN]
        list_result = [[] for i in repeat(None,len(list_id))]

        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT 10
            ''', [list_id[i]])
            rows_count = self.cursor.rowcount

            if (rows_count == 10): # 10 is LIMIT of rows in the query
                for doc in self.cursor:
                   list_result[i].append(int(doc[0]))
            elif (rows_count > 0 and rows_count < 10):
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
                list_result[i] = list_result[i] + [0] * (10 - rows_count)
            else:
                list_result[i] = [0] * 10

            list_result[i].reverse()

#         print "TESTING .... {}".format(list_result)
        return list_result

    def read_profile_change(self):
        """
            Read changes from profile
        """
        from itertools import repeat

        self.ID_NUM_CHANGE_DES    = kpi_from_db_config.ID_NUM_CHANGE_DES
        self.ID_NUM_CHANGE_AVATAR = kpi_from_db_config.ID_NUM_CHANGE_AVATAR
        self.ID_NUM_CHANGE_LOC    = kpi_from_db_config.ID_NUM_CHANGE_LOC
        self.ID_NUM_CHANGE_NAME   = kpi_from_db_config.ID_NUM_CHANGE_NAME
        self.ID_NUM_CHANGE_URL    = kpi_from_db_config.ID_NUM_CHANGE_URL
        self.ID_NUM_CHANGE_ANY    = kpi_from_db_config.ID_NUM_CHANGE_ANY

        list_id = [self.ID_NUM_CHANGE_DES, 
                    self.ID_NUM_CHANGE_AVATAR, 
                    self.ID_NUM_CHANGE_LOC, 
                    self.ID_NUM_CHANGE_NAME, 
                    self.ID_NUM_CHANGE_URL, 
                    self.ID_NUM_CHANGE_ANY]
        list_result = [[] for i in repeat(None,len(list_id))]

        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT 2
            ''', [list_id[i]])
            rows_count = self.cursor.rowcount
            
            if (rows_count == 2): # 2 is LIMIT from the query
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
            elif (rows_count == 1): # Change rows_count > 0 and rows_count < Number of limit
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
                list_result[i] = list_result[i] + [0]   
            else:
                list_result[i] = [0] * 2


#         print "TESTING .... {}".format(list_result)
        return list_result

    def read_weekly_source_report(self):
        """
            Read weekly source report
        """
        from itertools import repeat

        self.ID_SOURCE_1DES_1URL         = kpi_from_db_config.ID_SOURCE_1DES_1URL
        self.ID_SOURCE_1DES_0URL         = kpi_from_db_config.ID_SOURCE_1DES_0URL
        self.ID_SOURCE_0DES_1URL         = kpi_from_db_config.ID_SOURCE_0DES_1URL
        self.ID_SOURCE_0DES_0URL         = kpi_from_db_config.ID_SOURCE_0DES_0URL
        self.ID_SOURCE_0DES_GT200        = kpi_from_db_config.ID_SOURCE_0DES_GT200
        self.ID_SOURCE_0DES_LT100        = kpi_from_db_config.ID_SOURCE_0DES_LT100
        self.ID_SOURCE_0DES_GTE100_LT200 = kpi_from_db_config.ID_SOURCE_0DES_GTE100_LT200

        list_id = [self.ID_SOURCE_1DES_1URL, 
                    self.ID_SOURCE_1DES_0URL, 
                    self.ID_SOURCE_0DES_1URL, 
                    self.ID_SOURCE_0DES_0URL, 
                    self.ID_SOURCE_0DES_GT200, 
                    self.ID_SOURCE_0DES_GTE100_LT200, 
                    self.ID_SOURCE_0DES_LT100]
        len_need_list = [6, 6, 6, 6, 7, 7, 7]
        list_result = [[] for i in repeat(None,len(list_id))]
#         list_result = []
        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT %s
            ''', [list_id[i], 2*len_need_list[i]])
            rows_count = self.cursor.rowcount

            if (rows_count == 2*len_need_list[i]): # 2*len_need_list[i] is LIMIT from the query
                temp_list = []
                for doc in self.cursor:
                    if (len(list_result[i]) < len_need_list[i]):
                        list_result[i].insert(0, int(doc[0]))
                    elif (len(list_result[i]) == len_need_list[i]):
                        list_result[i].append(sum(list_result[i]))
                        temp_list.insert(0, int(doc[0]))
                    else:
                        temp_list.insert(0, int(doc[0]))
                temp_list.append(sum(temp_list))        
                list_result[i].extend(temp_list)
            elif (rows_count > 0 and rows_count < 2*len_need_list[i]): # Change rows_count > 0 and rows_count < Number of limit
                temp_list = []
                for doc in self.cursor:
                    if (len(list_result[i]) < len_need_list[i]):
                        list_result[i].insert(0, int(doc[0]))
                    elif (len(list_result[i]) == len_need_list[i]):
                        list_result[i].append(sum(list_result[i]))
                        temp_list.insert(0, int(doc[0]))
                    else:
                        temp_list.insert(0, int(doc[0]))
                list_result[i] = list_result[i] + [0] * (2*len_need_list[i] + 2 - rows_count)   
            else:
                list_result[i] = [0] * (2*len_need_list[i] + 2)
#             list_result.append([])
#             for doc in self.cursor:
#                 list_result[-1].append(doc[0])
# 
#             while len(list_result[-1]) < len_need_list[i]:
#                 list_result[-1].append(0)
# 
#             list_result[-1][0:len_need_list[i]] = list_result[-1][::-1][len_need_list[i]:2*len_need_list[i]]
#             list_result[-1][len_need_list[i]:2*len_need_list[i]] = list_result[-1][::-1][0:len_need_list[i]]
# 
#             list_result[-1][len_need_list[i]:len_need_list[i]] = [sum(list_result[-1][0:len_need_list[i]])]
#             list_result[-1].append(sum(list_result[-1][len_need_list[i] + 1:2*len_need_list[1]- 1]))

#         print "TESTING .... len = {} and {}".format(len(list_result), list_result)
        return list_result

    def read_weekly_publisher_report(self):
        """
            Read weekly publisher report
        """
        from itertools import repeat

        self.ID_TWITTER_BROKEN_AVATAR      = kpi_from_db_config.ID_TWITTER_BROKEN_AVATAR
        self.ID_TWITTER_MISSING_AVATAR     = kpi_from_db_config.ID_TWITTER_MISSING_AVATAR
        self.ID_TWITTER_MISSING_ISO        = kpi_from_db_config.ID_TWITTER_MISSING_ISO
        self.ID_TWITTER_HAS_MANY_ITEMKEY   = kpi_from_db_config.ID_TWITTER_HAS_MANY_ITEMKEY
        self.ID_TWITTER_HAS_MANY_PUBLISHER = kpi_from_db_config.ID_TWITTER_HAS_MANY_PUBLISHER
        self.ID_TWITTER_WITHOUT_ITEMKEY    = kpi_from_db_config.ID_TWITTER_WITHOUT_ITEMKEY

        list_id = [self.ID_TWITTER_BROKEN_AVATAR, 
                    self.ID_TWITTER_MISSING_AVATAR, 
                    self.ID_TWITTER_MISSING_ISO, 
                    self.ID_TWITTER_HAS_MANY_ITEMKEY, 
                    self.ID_TWITTER_HAS_MANY_PUBLISHER, 
                    self.ID_TWITTER_WITHOUT_ITEMKEY]
        list_result = [[] for i in repeat(None,len(list_id))]
        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT 2
            ''', [list_id[i]])
            rows_count = self.cursor.rowcount
            
            if (rows_count == 2): # 2 is LIMIT from the query
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
            elif (rows_count == 1): # Change rows_count > 0 and rows_count < Number of limit
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
                list_result[i] = list_result[i] + [0]   
            else:
                list_result[i] = [0] * 2

        return list_result

    def read_weekly_breakdown_statuses(self):
        """
            Read weekly breakdown statuses
        """
        from itertools import repeat

        self.ID7_GNIP_BREAKDOWN    = kpi_from_db_config.ID7_GNIP_BREAKDOWN
        self.ID7_STREAM_BREAKDOWN  = kpi_from_db_config.ID7_STREAM_BREAKDOWN
        self.ID7_SEED_BREAKDOWN    = kpi_from_db_config.ID7_SEED_BREAKDOWN
        self.ID7_MENTION_BREAKDOWN = kpi_from_db_config.ID7_MENTION_BREAKDOWN
               
        list_id = [self.ID7_GNIP_BREAKDOWN, 
                    self.ID7_STREAM_BREAKDOWN, 
                    self.ID7_SEED_BREAKDOWN, 
                    self.ID7_MENTION_BREAKDOWN]

        list_result = [[] for i in repeat(None,len(list_id))]
        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT 6
            ''', [list_id[i]])
            rows_count = self.cursor.rowcount
            
            if (rows_count == 6): # 6 is LIMIT from the query
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
            elif (rows_count >= 1 and rows_count < 6): # Change rows_count > 0 and rows_count < Number of limit
                for doc in self.cursor:
                    list_result[i].append(int(doc[0]))
                list_result[i] = list_result[i] + [0] * (6 - rows_count)   
            else:
                list_result[i] = [0] * 6

        return list_result

    def read_publisher_snapshot(self):
        """
            Read publisher snapshot
        """
        size_m = 42
        size_n = 16
        list_id = self.ID_PUBLISHER_SNAPSHOT

        self.cursor.execute('''
            SELECT value
            FROM public.kpi_report
            WHERE id = %s
            ORDER BY created_at DESC
            LIMIT %s
        ''', [list_id, size_m*size_n])

        list_result = [[0]*size_n for _ in range(size_m)]

        count_result = size_m*size_n - 1
        for doc in self.cursor:
            list_result[count_result/size_n][count_result%size_n] = doc[0]
            count_result -= 1

        categories = ['P',
                    'P>Financial Market Professionals',
                    'P>Financial Market Professionals>Financial Advisor',
                    'P>Financial Market Professionals>Financial Advisor>Economist',
                    'P>Financial Market Professionals>Financial Advisor>Financial Advisor - Other',
                    'P>Financial Market Professionals>Financial Advisor>Financial Planner',
                    'P>Financial Market Professionals>Financial Analyst',
                    'P>Financial Market Professionals>Financial Analyst>Buy Side Analyst',
                    'P>Financial Market Professionals>Financial Analyst>Financial Analyst - Other',
                    'P>Financial Market Professionals>Financial Analyst>Sell Side Analyst',
                    'P>Financial Market Professionals>Financial Professional - Other',
                    'P>Financial Market Professionals>Financial Professional - Other>Financial Professional - Other',
                    'P>Financial Market Professionals>Fin Executive',
                    'P>Financial Market Professionals>Fin Executive>CEO - Fin Org',
                    'P>Financial Market Professionals>Fin Executive>CFO',
                    'P>Financial Market Professionals>Fin Executive>Other Executive - Fin Org',
                    'P>Financial Market Professionals>Fund/Portfolio Manager',
                    'P>Financial Market Professionals>Investor Relations Professional',
                    'P>Financial Market Professionals>Risk Professional',
                    'P>Financial Market Professionals>Trader',
                    'P>Financial Market Professionals>Trader>Commodity Trader',
                    'P>Financial Market Professionals>Trader>Currency Trader',
                    'P>Financial Market Professionals>Trader>Multi-Asset-Class Trader',
                    'P>Financial Market Professionals>Trader>Stock Trader',
                    'P>Financial Market Professionals>Trader>Trader - Other',
                    'P>Non-identified Persons',
                    'P>Other Stakeholders',
                    'P>Other Stakeholders>Activist',
                    'P>Other Stakeholders>Corporate Communication',
                    'P>Other Stakeholders>Educational Professional',
                    'P>Other Stakeholders>Educational Professional>Educational Professional - Business & Finance',
                    'P>Other Stakeholders>Educational Professional>Educational Professional - Other',
                    'P>Other Stakeholders>Educational Professional>Educational Professional - Technology',
                    'P>Other Stakeholders>Job Recruiter',
                    'P>Other Stakeholders>Journalist',
                    'P>Other Stakeholders>Journalist>Business & Finance Journalist',
                    'P>Other Stakeholders>Journalist>Journalist - Other',
                    'P>Other Stakeholders>Journalist>Technology Journalist',
                    'P>Other Stakeholders>Lawyer',
                    'P>Other Stakeholders>Non-Financial Executive',
                    'P>Other Stakeholders>Other Person',
                    'P>Other Stakeholders>Politician']

        return list_result, categories

    def read_publisher_qualified_isocode(self):
        """
            Read qualified publisher iso code
        """
        self.CATEGORIES                     = kpi_from_db_config.CATEGORIES
        self.SELECTED_ISOCODE               = kpi_from_db_config.SELECTED_ISOCODE
        self.ID_PUBLISHER_QUALIFIED_ISOCODE = kpi_from_db_config.ID_PUBLISHER_QUALIFIED_ISOCODE

        size_m = len(self.CATEGORIES)
        size_n = len(self.SELECTED_ISOCODE)
        list_id = self.ID_PUBLISHER_QUALIFIED_ISOCODE

        self.cursor.execute('''
            SELECT value
            FROM public.kpi_report
            WHERE id = %s
            ORDER BY created_at DESC
            LIMIT %s
        ''', [list_id, size_m*size_n])

        list_result = [[0]*size_n for _ in range(size_m)]

        count_result = size_m*size_n - 1
        for doc in self.cursor:
            list_result[count_result%size_m][count_result/size_m] = doc[0]
            count_result -= 1

        for i in range(size_m):
            list_result[i][0:0] = [sum([list_result[i][j] for j in range(size_n)])]

        list_result[0:0] = [[0]*(size_n + 1)]
        for i in range(size_n + 1):
            list_result[0][i] = sum([list_result[j][i] for j in range(size_m + 1)])

        categories = ['--All--'] + self.CATEGORIES

        return list_result, categories

    def change_len_kpi(self):
        """
            Change length of kpi
        """
        self.ID_SOURCE_1DES_1URL         = kpi_from_db_config.ID_SOURCE_1DES_1URL
        self.ID_SOURCE_1DES_0URL         = kpi_from_db_config.ID_SOURCE_1DES_0URL
        self.ID_SOURCE_0DES_1URL         = kpi_from_db_config.ID_SOURCE_0DES_1URL
        self.ID_SOURCE_0DES_0URL         = kpi_from_db_config.ID_SOURCE_0DES_0URL
        self.ID_SOURCE_0DES_GT200        = kpi_from_db_config.ID_SOURCE_0DES_GT200
        self.ID_SOURCE_0DES_LT100        = kpi_from_db_config.ID_SOURCE_0DES_LT100
        self.ID_SOURCE_0DES_GTE100_LT200 = kpi_from_db_config.ID_SOURCE_0DES_GTE100_LT200

        list_id = [self.ID_SOURCE_1DES_1URL, 
                    self.ID_SOURCE_1DES_0URL, 
                    self.ID_SOURCE_0DES_1URL, 
                    self.ID_SOURCE_0DES_0URL, 
                    self.ID_SOURCE_0DES_GT200, 
                    self.ID_SOURCE_0DES_GTE100_LT200, 
                    self.ID_SOURCE_0DES_LT100]
        old_len_need_list = [4, 4, 4, 4, 5, 5, 5]
        for i in range(len(list_id)):
            self.cursor.execute('''
                SELECT value
                FROM public.kpi_report
                WHERE id = %s
                ORDER BY created_at DESC
                LIMIT %s
            ''', [list_id[i], 2*old_len_need_list[i]])

            list_result = []
            for doc in self.cursor:
                list_result.append(doc[0])

            new_list_result = list_result
            new_list_result[3:3] = [0, 0]
            if i < 4:
                new_list_result[9:9] = [0, 0]
            else:
                new_list_result[10:10] = [0, 0]

            for new_value in new_list_result[::-1]:
                self.cursor.execute('''
                    INSERT INTO public.kpi_report(id, created_at, value)
                    VALUES(%s, now(), %s)
                ''', [list_id[i], new_value])
                self.con_dev.commit()

    def close_connection(self):
        self.cursor.close()

if __name__ == '__main__':
    kpi_from_db = KPIFromDb()
    kpi_from_db.change_len_kpi()
    kpi_from_db.close_connection()
