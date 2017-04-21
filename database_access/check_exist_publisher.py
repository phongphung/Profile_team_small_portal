__author__ = 'sunary'


import os
from utils import my_connection, my_helper, my_text
import pandas as pd
from twitter.twitter_services import TwitterService


class CheckExistPublisher():

    def __init__(self):
        self.con_psql1 = my_connection.psql1_connection()

    def process_create_new_file(self):
        pd_file = pd.read_csv('/home/nhat.vo/10Sep2015_95k new twitter id.csv')

        fo = open('/home/nhat.vo/exist_db.csv', 'w')
        fo.write('id,exist_in_db\n')
        for id in pd_file['id']:
            exist = self.exist_in_publisher(str(id))
            fo.write(str(id) + ',' + str(exist) + '\n')
        fo.close()

    def process_override_old_file(self):
        pd_file = pd.read_csv('/home/nhat/data/10Sep2015_95k new twitter id.csv')
        dataframe_fields = my_helper.get_dataframe_columns('/home/nhat/data/10Sep2015_95k new twitter id.csv')
        dataframe_fields.append('exist_in_db')

        dataframe = {}
        for f in dataframe_fields:
            dataframe[f] = []

        for i in range(len(pd_file['id'][:1])):
            for f in dataframe_fields[:-1]:
                dataframe[f].append(my_helper.except_pandas_value(pd_file[f][i]))

            dataframe[dataframe_fields[-1]].append(self.exist_in_publisher(str(pd_file['id'][i])))

        dataframe = pd.DataFrame(data=dataframe, index=None, columns=dataframe_fields)
        dataframe.to_csv('/home/nhat/data/exist_db.csv', index=False)

    def check_muckrack(self):
        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))

        pd_file = pd.read_csv('/home/nhat.vo/muckrack.csv')
        dataframe_fields = my_helper.get_dataframe_columns('/home/nhat.vo/muckrack.csv')

        dataframe_fields += ['screen_name', 'str_id', 'exist_db']

        dataframe = {}
        for f in dataframe_fields:
            dataframe[f] = []

        list_screen_names = []
        for i in range(len(pd_file[dataframe_fields[0]])):
            for f in dataframe_fields[:-3]:
                dataframe[f].append(my_helper.except_pandas_value(pd_file[f][i]))

            if not my_helper.pandas_null(pd_file['twitter'][i]):
                screen_name = my_text.username_from_url(pd_file['twitter'][i])
                dataframe['screen_name'].append(screen_name)
                if screen_name:
                    list_screen_names.append(screen_name)
            else:
                dataframe['screen_name'].append('')

        new_list_ids, new_list_screen_names = twitter_service.get_ids_screen_names(list_screen_names=list_screen_names)

        id_twitter_info = 0
        for i in range(len(dataframe[dataframe_fields[0]])):
            if dataframe['screen_name'][i] and new_list_ids[id_twitter_info]:
                dataframe['str_id'].append(new_list_ids[id_twitter_info])
                dataframe['exist_db'].append(self.exist_in_publisher(str(dataframe['str_id'][i])))
            else:
                dataframe['str_id'].append('')
                dataframe['exist_db'].append(False)

            if dataframe['screen_name'][i]:
                id_twitter_info += 1

        dataframe = pd.DataFrame(data=dataframe, columns=dataframe_fields)
        dataframe.to_csv('/home/nhat.vo/muckrack_exist_db.csv', index=False)

    def exist_in_publisher(self, str_id):
        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT True
            FROM mg_publisher_sns_clean
            WHERE sns_id = %s
                AND sns_name = 'tw'
        ''', [str_id])
        return True if cur_psql1.rowcount else False

    def close_connection(self):
        self.con_psql1.close()


if __name__ == '__main__':
    check_exist_publisher = CheckExistPublisher()
    check_exist_publisher.check_muckrack()
    check_exist_publisher.close_connection()