__author__ = 'sunary'


import pandas as pd
from utils import my_connection
import ast
from operator import itemgetter


class GetCategory():

    def __init__(self):
        pass

    def get_id(self):
        con_da0 = my_connection.da0_connection()
        cur_da0 = con_da0.cursor()
        cur_da0.execute('''
            SELECT user_id
            FROM twitter.user_source
            WHERE source_id = 4
        ''')

        self.res = cur_da0.fetchall()
        con_da0.close()

    def get_category(self):
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        df_category = {'user_id': [],
                       'cat': []}

        for id_seed, in self.res:
            cur_psql1.execute('''
                SELECT sns_id,
                        object_payload->>'categories'
                FROM mg_publisher
                    INNER JOIN mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
                WHERE sns_name = 'tw'
                    AND sns_id = %s
            ''',[str(id_seed)])
            res_cat = cur_psql1.fetchone()
            if res_cat:
                df_category['user_id'].append(res_cat[0])
                list_cat = ''
                if res_cat[1]:
                    res_cat = ast.literal_eval(res_cat[1].replace('null', '""'))
                    res_cat = sorted(res_cat, key=itemgetter('level'))
                    for cat in res_cat:
                        list_cat += cat.get('name') + ', '
                df_category['cat'].append(list_cat)

        con_psql1.close()

        df_category = pd.DataFrame(data=df_category, columns=['user_id', 'cat'])
        df_category.to_csv('category.csv', index= False)

    def save_into_csv(self):
        pd_file = pd.read_csv('/home/sunary/data_report/result/sql_seed.csv')
        pd_file_cat = pd.read_csv('/home/sunary/data_report/result/category.csv')

        df_profile_cat = {}
        dataframe_fields = ['user_id', 'name', 'screen_name', 'description', 'location', 'url', 'lang', 'statuses_count', 'profile_image_url', 'time_zone', 'isocode', 'count_follower', 'count_following', 'cat']
        for f in dataframe_fields:
            df_profile_cat[f] = []

        for i in range(len(pd_file['user_id'])):
            for f in dataframe_fields[:-1]:
                df_profile_cat[f].append(pd_file[f][i])

            df_profile_cat[dataframe_fields[-1]].append('')
            for j in range(len(pd_file_cat['user_id'])):
                if pd_file['user_id'][i] == pd_file_cat['user_id'][j]:
                    df_profile_cat[dataframe_fields[-1]][-1] = pd_file_cat['cat'][j]
                    break

        pd_file = pd.DataFrame(data=df_profile_cat, index=None, columns=dataframe_fields)
        pd_file.to_csv('/home/sunary/data_report/result/category_profile.csv', index= False)

if __name__ == '__main__':
    get_category = GetCategory()
    # get_category.get_id()
    # get_category.get_category()
    get_category.save_into_csv()
