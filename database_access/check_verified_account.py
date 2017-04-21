__author__ = 'sunary'


import pandas as pd
from utils import my_connection


class CheckVerifiedAccount():

    def __init__(self):
        # self.mongo6 = Mongodb(host='mongo6.ssh.sentifi.com', port=27331, db='Twitter', col='User')
        self.con_psql1 = my_connection.psql1_connection()

    def process(self):
        pd_file = pd.read_csv('/home/nhat/data/27Jul2015_cheeck_verified_account_167k.csv')

        fo = open('/home/nhat/data/result/check_verified.csv', 'w')
        for i in range(len(pd_file['id'])):
            verified = self.check_verified_psql1(str(pd_file['id'][i]))
            fo.write(str(pd_file['screen_name'][i]) + ',' + str(pd_file['id'][i]) + ',' + str(verified) + '\n')
        fo.close()

    def check_verified_mongo6(self, user_id):
        res = self.mongo6.find({'id_str': user_id}, ['verified'], sort=[('_id', -1)], limit=1)
        if res:
            try:
                return res.next()['verified']
            except Exception, e:
                print e
        return False

    def check_verified_psql1(self, user_id):
        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT object_payload->'verified'
            FROM mg_twitter
            WHERE object_payload->>'id_str' = %s
        ''', [user_id])

        if cur_psql1.rowcount:
            try:
                return cur_psql1.fetchone()[0]
            except Exception, e:
                print e
        return False

    def close_connection(self):
        self.con_psql1.close()

if __name__ == '__main__':
    check_verified_account = CheckVerifiedAccount()
    check_verified_account.process()
    check_verified_account.close_connection()
