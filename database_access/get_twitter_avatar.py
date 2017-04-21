__author__ = 'sunary'


from utils.my_mongo import Mongodb
import pandas as pd
from utils import my_helper, my_connection


class GetTwitterAvatar():

    def __init__(self):
        pass

    def get_publisher_avatar(self):
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        mongo = Mongodb(db= 'publisher_avatar', col= 'avatar_2006')

        pd_file = pd.read_csv('Toprank1000_photourl.csv')
        for i in range(len(pd_file['twitter id'])):
            cur_psql1.execute('''
                SELECT object_payload->>'image'
                FROM mg_publisher
                    INNER JOIN mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
                WHERE sns_name = 'tw'
                    AND sns_id = %s
            ''', [str(pd_file['sns_id'][i])])
            if cur_psql1.rowcount:
                data = {'screenname': pd_file['screenname'][i],
                        'id': pd_file['twitter id'][i],
                        'avatar': cur_psql1.fetchone()[0],
                        'valid': False}
                # if my_helper.check_exist_url(cur_psql1.fetchone()[0]):
                #     data['valid'] = True
                mongo.insert(data)

        con_psql1.close()

    def get_twitter_info(self):
        '''
        get user_id, screen_name, name, avatar_url
        by user_id
        from mg_publisher
        '''
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        twitter_info = []
        pd_file = pd.read_csv('/home/nhat/data/7Aug2015_3700 publishers_CreditSuisse.csv')
        for i in range(len(pd_file['sns_id'])):
            cur_psql1.execute('''
                SELECT object_payload->>'image'
                FROM mg_publisher
                    INNER JOIN mg_publisher_sns_clean ON (object_id = publisher_mongo_id)
                WHERE sns_name = 'tw'
                    AND sns_id = %s
            ''', [str(pd_file['sns_id'][i])])
            data = {}
            if cur_psql1.rowcount:
                info = cur_psql1.fetchone()
                data = {'id': pd_file['sns_id'][i],
                        'old-avatar': info[0],
                        'valid-old-avatar':  my_helper.check_exist_url(info[0])}

            cur_psql1.execute('''
                SELECT object_payload->>'profile_image_url_https',
                    object_payload->>'screen_name',
                    object_payload->>'name'
                FROM mg_twitter
                WHERE object_payload->>'id_str' = %s
            ''', [str(pd_file['sns_id'][i])])

            if cur_psql1.rowcount:
                info = cur_psql1.fetchone()
                if not data['valid-old-avatar']:
                    data['new-avatar'] = info[0]
                    data['valid-new-avatar'] = my_helper.check_exist_url(info[0])
                else:
                    data['new-avatar'] = ''
                    data['valid-new-avatar'] = ''
                data['screen_name'] = info[1]
                data['name'] = info[2]
            else:
                data['new-avatar'] = ''
                data['valid-new-avatar'] = ''
                data['screen_name'] = ''
                data['name'] = ''

            if data:
                twitter_info.append(data)

        fo = open('/home/nhat/data/result/check_avatar.csv', 'w')
        for data in twitter_info:
            fo.write(str(data.get('id')) + ',' + str(data.get('screen_name')) + ',' + str(data.get('name')) + ',' + str(data.get('old-avatar')) + ',' + str(data.get('valid-old-avatar')) + ',' + str(data.get('new-avatar')) + ',' + str(data.get('valid-new-avatar')) + '\n')

        fo.close()
        con_psql1.close()

    def check_avatar(self):
        '''
        check publisher avatar
        write to csv
        '''
        fo = open('/home/nhat/data/result/check_avatar.csv', 'w')
        pd_file = pd.read_csv('/home/nhat/data/6Jul2015_Top 15k _Check avatar.csv')
        for i in range(len(pd_file['photo'])):
            fo.write(pd_file['photo'][i] + ',' + str(pd_file['tw-general'][i]) + ',')
            if my_helper.pandas_null(pd_file['photo'][i]) or not my_helper.check_exist_url(pd_file['photo'][i]):
                fo.write('0\n')
            else:
                fo.write('1\n')

        fo.close()

    def get_newest_avatar(self):
        '''
        get newest avatar
        write to csv
        '''
        self.mongo6 = Mongodb(host='mongo6.ssh.sentifi.com', port=27331, db='Twitter', col='User')
        fo = open('/home/nhat/data/result/add_avatar.csv', 'w')
        pd_file = pd.read_csv('/home/nhat/data/result/check_avatar.csv')

        for i in range(len(pd_file['photo'])):
            if str(pd_file['status'][i]) == '0':
                id = pd_file['tw-general'][i].split('{')[1][:-1]
                fo.write(self.check_avatar_in_mongo6(id) + ',' + str(pd_file['tw-general'][i]))

        fo.close()

    def check_avatar_in_mongo6(self, id):
        '''
        get newest avatar in mongo6 and check exist
        '''
        res = self.mongo6.find({'id_str': id}, ['profile_image_url_https'], sort=[('_id', -1)], limit=1)
        if res:
            try:
                url = res.next()['profile_image_url_https']
                if url and my_helper.check_exist_url(url):
                    return url
            except:
                return ''
        return ''

if __name__ == '__main__':
    get_avatar = GetTwitterAvatar()
    get_avatar.check_avatar()