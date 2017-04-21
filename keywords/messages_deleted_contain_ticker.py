__author__ = 'sunary'


from utils import my_connection
import requests
import pandas as pd


class MsgDeletedContainTicker():

    def __init__(self):
        self.con_psql1 = my_connection.ellytran_connection()

    def process(self, file_source, file_dest):
        pd_file = pd.read_csv(file_source)

        writer = 'item_id,ticker,exist_in_top_msg\n'
        for i in range(len(pd_file['id'])):
            self.get_word_bag(pd_file['id'][i])
            self.get_ticker(pd_file['id'][i])
            self.top_messages = []

            for tk in self.tickers:
                if tk and '$' + tk not in self.keywords:
                    if not self.top_messages:
                        self.get_top_messages(pd_file['id'][i])
                    if tk in self.top_messages:
                        writer += '%s,%s,co\n' % (pd_file['id'][i], tk)
                    else:
                        writer += '%s,%s,khong\n' % (pd_file['id'][i], tk)

        with open(file_dest, 'w') as fo:
            fo.write(writer)

        self.close_connection()

    def one_case(self, item_id):
        self.get_word_bag(item_id)
        self.get_ticker(item_id)
        self.top_messages = []

        print 'tickers: %s' % self.tickers
        print 'keywords: %s' % self.keywords
        for tk in self.tickers:
            if '$' + tk not in self.keywords:
                if not self.top_messages:
                    self.get_top_messages(item_id)
                print self.top_messages

                if tk in self.top_messages:
                    print 'co'
                else:
                    print 'khong'

        self.close_connection()

    def get_word_bag(self, item_id):
        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT active_word_bag,
                semantic_only_word_bag,
                disabled_word_bag
            FROM expression
            WHERE status = 1
                AND item_id = %s
        ''', [item_id])

        self.keywords = []

        if cur_psql1.rowcount:
            doc = cur_psql1.fetchone()
            if doc[0]: self.keywords += [word.get('word').lower() for word in doc[0]]
            if doc[1]: self.keywords += [word.get('word').lower() for word in doc[1]]
            if doc[2]: self.keywords += [word.get('word').lower() for word in doc[2]]

    def get_ticker(self, item_id):
        cur_psql1 = self.con_psql1.cursor()
        cur_psql1.execute('''
            SELECT bloomberg_ticker,
                google_finance_ticker,
                reuters_ticker,
                local_ticker
            FROM public.ticker1
            WHERE item_id = %s
                AND status = 1
        ''', [item_id])

        self.tickers = set()

        if cur_psql1.rowcount:
            doc = cur_psql1.fetchone()
            if doc[0]: self.tickers |= set([x.lower() for x in doc[0]])
            if doc[1]: self.tickers |= set([x.lower() for x in doc[1]])
            if doc[2]: self.tickers |= set([x.lower() for x in doc[2]])
            if doc[3]: self.tickers |= set([x.lower() for x in doc[3]])

    def get_top_messages(self, item_id):
        res = requests.get('http://filter.ssh.sentifi.com:10010/message/topmessagenew?top=40&channel=twitter&isNew=true&itemkey=%s' % (item_id))
        try:
            self.top_messages = [x['message'].lower() for x in res.json()['data']['all']]
            self.top_messages = reduce(lambda x, y: x + y, self.top_messages)
        except:
            self.top_messages = '_'

    def close_connection(self):
        self.con_psql1.close()

if __name__ == '__main__':
    msg_deleted_contain_ticker = MsgDeletedContainTicker()
    # msg_deleted_contain_ticker.one_case(4384)
    msg_deleted_contain_ticker.process('/home/nhat.vo/id uk.csv', '/home/nhat.vo/check_deleted_keyword.csv')