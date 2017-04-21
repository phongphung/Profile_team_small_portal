__author__ = 'sunary'


import re
import pandas as pd
import urllib
from utils.my_helper import init_logger
from utils.my_mongo import Mongodb


class CheckCode():

    def __init__(self):
        pass

    def replce_objectid(self):
        mongodb_save = Mongodb(host='localhost',
                                db='linkedin_user_data',
                                col='user')
        mongodb_new = Mongodb(host='localhost',
                                db='linkedin_user_data',
                                col='info')

        file_list_ids = pd.read_csv('/home/sunary/data_report/list_url_linkedin_20150415.csv')
        list_id_account = file_list_ids['_id']
        list_urls = []
        #format url
        for i in range(12000):
            list_urls.append(self.format_url(file_list_ids['url'][i]))


        old_data = mongodb_save.find()
        print 'here'
        for data in old_data:
            for i in range(12000):
                if list_urls[i].decode('utf-8') in data['url'].decode('utf-8'):
                    data['_id'] = list_id_account[i]
                    print data['url']
                    if mongodb_new.count({'_id': data['_id']}) <= 0:
                        mongodb_new.insert(data)
                    break

    def format_url(self, url):
        link = re.search('.*(linkedin.*)', url)
        return 'https://www.' + link.group(1) if link else ''

    def get_ticker(self):
        string = '{aaa:bbb}'
        pattern = '{(.+)\:(.+)}'
        print re.sub(pattern, '{\2}', string)

    def check_del(self):
        mongodb_save = Mongodb(host='localhost',
                                db='linkedin_user_data',
                                col='new')
        res = mongodb_save.find({}, ['_id'])
        print res[0].get('_id')

    def check_decode_url(self):
        url = 'https://touch.www.linkedin.com?sessionid=8245740851691520&trk=vsrp_people_res_name&redirectUrl=%23profile%2F45502328%2Fname%3AWwn0&spcon=profile&rs=false#profile/45502328/name:Wwn0'
        print urllib.unquote_plus(url)

    def dict_list(self, source_dict):
        dest_list = []
        for i in range(len(source_dict)):
            dest_list.append(source_dict[i])
        return dest_list

    def del_stt(self):
        file_csv = open('/home/sunary/data_report/new_follower.csv', 'r')
        text_csv = file_csv.read()
        file_csv.close()

        text_csv = re.sub('\n(\d+,)','\n', text_csv)
        file_csv = open('/home/sunary/data_report/new2_follower.csv', 'w')
        file_csv.write(text_csv)

    def check_dataframe(self):
        dict_data = {'id':[], 'location': []}
        for id in range(10):
            dict_data['id'].append('0')
            dict_data['location'].append('1')
        print dict_data

    def count_loop(self):
        x = 0
        for i1 in range(1, 4):
            for i2 in range(i1 + 1):
                for i3 in range(i2 + 1):
                    x += 1
        print x

    def vary(self, list_words, max_combine = None):
        '''
        Examples:
            >>> vary(['a', 'b', 'c'])
            [['a', 'b', 'c'], ['ab', 'c'], ['a', 'bc'], ['abc']]
        '''
        group_list_words = [list_words]
        if max_combine is None:
            max_combine = len(list_words)
        else:
            max_combine = min(len(list_words), max_combine)

        last_group = group_list_words
        for i in range(max_combine - 1):
            temp_group = last_group
            last_group = []
            for gr in temp_group:
                get_group = self.group2words(gr)
                for gr in get_group:
                    if gr not in group_list_words and gr not in last_group:
                        last_group .append(gr)

            group_list_words += last_group

        return group_list_words

    def group2words(self, list_words):
        '''
        Examples:
            >>> group2words(['a', 'b', 'c'])
            [['ab', 'c'], ['a', 'bc']]
        '''
        group_words = []
        for i in range(0, len(list_words) - 1):
            group_words.append(list_words[0:i] + [''.join(list_words[i:i+2])] + list_words[i+2:])

        return group_words

    def check_log(self):
        logger = init_logger(self.__class__.__name__)
        logger.info('test')


if __name__ == '__main__':
    test = CheckCode()
    print test.vary(['a', 'b', 'c', 'd'], 2)