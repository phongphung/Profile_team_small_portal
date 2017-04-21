__author__ = 'sunary'


import pandas as pd
from utils import my_connection


class GetItemId():
    def __init__(self):
        self.con_psql1 = my_connection.psql1_connection()

    def read_id(self):
        pd_file = pd.read_csv('20May2015_BlackRock&HSBC_for_Nhat to crawl item key.csv')

        self.list_id_item = {'id': [],
                             'item_id': []}
        for id in pd_file['sns_id']:
            self.list_id_item['id'].append(id)
        pass

    def read_id_full(self):
        pd_file = pd.read_csv('talkANZ.csv')

        self.list_id_item = {'0id': [],
                             '1item_id': [],
                             '2name': [],
                             '3screen_name': [],
                             '4description': [],
                             '5location': [],
                             '6url': [],
                             '7lang': [],
                             '8statuses_count': [],
                             '9profile_image_url': [],
                             'atime_zone': [],
                             'bisocode': [],
                             'cexist_db': [],
                              }
        for i in range(len(pd_file['user_id'])):
            self.list_id_item['0id'].append(pd_file['user_id'][i])
            self.list_id_item['2name'].append(pd_file['name'][i])
            self.list_id_item['3screen_name'].append(pd_file['screen_name'][i])
            self.list_id_item['4description'].append(pd_file['description'][i])
            self.list_id_item['5location'].append(pd_file['location'][i])
            self.list_id_item['6url'].append(pd_file['url'][i])
            self.list_id_item['7lang'].append(pd_file['lang'][i])
            self.list_id_item['8statuses_count'].append(pd_file['statuses_count'][i])
            self.list_id_item['9profile_image_url'].append(pd_file['profile_image_url'][i])
            self.list_id_item['atime_zone'].append(pd_file['time_zone'][i])
            self.list_id_item['bisocode'].append(pd_file['isocode'][i])
            self.list_id_item['cexist_db'].append(pd_file['exist_db'][i])
        pass

    def get_id_item(self):
        for id in self.list_id_item['0id']:
            cur_psql1 = self.con_psql1.cursor()
            try:
                cur_psql1.execute('''
                    SELECT item_id
                    FROM item_sns
                    WHERE (sns_name = 'tw' or sns_name = 'twitter' )
                        AND sns_id = %s
                ''', [str(id)])
            except:
                pass

            if cur_psql1.rowcount:
                items = ''
                for item_id in cur_psql1:
                    items += str(item_id[0])
                    if item_id != cur_psql1[-1]:
                        items += '-'
                self.list_id_item['1item_id'].append(items)
            else:
                self.list_id_item['1item_id'].append('')
            pass
        self.con_psql1.close()
        pass

    def write_id_item(self):
        pd_file = pd.DataFrame(data=self.list_id_item)
        pd_file.to_csv('list_item_id.csv', index=False)
        pass

if __name__ == '__main__':
    get_item_id = GetItemId()
    get_item_id.read_id_full()
    get_item_id.get_id_item()
    get_item_id.write_id_item()
