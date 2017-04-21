__author__ = 'sunary'


from utils import my_connection, my_helper
from utils.my_mongo import Mongodb
import requests
import gevent
import pandas as pd


class CountReleaseMessages():

    def __init__(self):
        self.client = my_connection.es_connection()

    def from_db(self, output_file):
        con_psql1 = my_connection.ellytran_connection()

        cur_psql1 = con_psql1.cursor()
        cur_psql1.execute('''
            SELECT id
            FROM item
                INNER JOIN item_audit USING (id)
            WHERE lower(profile_cat) = 'listed companies'
                AND profileready = 0
                AND ready = 0
                AND lower(isocode) = 'gb'
        ''')

        fo = open(output_file, 'w')
        fo.write('item_id,top_twitter,top_newsblog,total_twitter,total_newsblog\n')
        for item_id, in cur_psql1:
            count_top = self.top_relevant(item_id)
            count_total = self.total_relevant(item_id)
            fo.write(str(item_id) + ',' + str(count_top[0]) + ',' + str(count_top[1]) + ',' + str(count_total[0]) + ',' + str(count_total[1]) + '\n')

        fo.close()
        con_psql1.close()

    def from_csv(self, file_source, file_dest):
        original_fields = my_helper.get_dataframe_columns(file_source)

        generate_fields = original_fields[::]
        generate_fields.append('top_twitter')
        generate_fields.append('top_newsblog')
        generate_fields.append('total_twitter')
        generate_fields.append('total_newsblog')

        original_dataframe = pd.read_csv(file_source)
        generate_dataframe = {}
        for field in generate_fields:
            generate_dataframe[field] = []

        for i in range(len(original_dataframe['id'])):
            for j in range(len(original_fields)):
                generate_dataframe[generate_fields[j]].append(original_dataframe[original_fields[j]][i])

            counter = self.top_relevant(original_dataframe['id'][i])
            generate_dataframe[generate_fields[len(original_fields)]].append(counter[0])
            generate_dataframe[generate_fields[len(original_fields) + 1]].append(counter[1])

            counter = self.total_relevant(original_dataframe['id'][i])
            generate_dataframe[generate_fields[len(original_fields) + 2]].append(counter[0])
            generate_dataframe[generate_fields[len(original_fields) + 3]].append(counter[1])

        original_dataframe = pd.DataFrame(data=generate_dataframe, index=None, columns=generate_fields)
        original_dataframe.to_csv(file_dest, index= False)

    def from_csv_asynchronous(self, file_source, file_dest):
        self.mongo_counter = Mongodb(db='count_msg', col='count')
        self.mongo_counter.remove()

        self.original_fields = my_helper.get_dataframe_columns(file_source)

        self.generate_fields = self.original_fields[::]
        self.generate_fields.append('top_twitter')
        self.generate_fields.append('top_newsblog')
        self.generate_fields.append('total_twitter')
        self.generate_fields.append('total_newsblog')

        self.original_dataframe = pd.read_csv(file_source)

        threads = [gevent.spawn(self.task, i) for i in xrange(len(self.original_dataframe['id']))]
        gevent.joinall(threads)

        self.mongo_counter.export_csv(fields=self.generate_fields, output=file_dest)

    def task(self, id):
        data = {}
        for j in range(len(self.original_fields)):
            data[self.generate_fields[j]] = my_helper.except_pandas_value(self.original_dataframe[self.original_fields[j]][id])

        counter = self.top_relevant(self.original_dataframe['id'][id])
        data['top_twitter'] = counter[0]
        data['top_newsblog'] = counter[1]

        counter = self.total_relevant(self.original_dataframe['id'][id])
        data['total_twitter'] = counter[0]
        data['total_newsblog'] = counter[1]

        self.mongo_counter.insert(data)

    def top_relevant(self, item_id):
        channel = ['twitter', 'blog,news']
        top_num = []
        for ch in channel:
            res = requests.get('http://filter.ssh.sentifi.com:10010/message/topmessagenew?top=40&channel=%s&isNew=true&itemkey=%s' % (ch, item_id))
            top_num.append(len(res.json()['data']['all']))
        return top_num

    def total_relevant(self, item_id):
        term_channel = [{"term": {"channel": "twitter"}}, {"terms": {"channel": ["blog", "news"]}}]
        total_num = []
        for term in term_channel:
            query = {
                "query": {
                    "filtered": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "ne_mentions.id": item_id
                                        }
                                    },
                                    term,
                                    {
                                        "range": {
                                            "published_at": {
                                                "gte": "now-3M",
                                                "lte": "now"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
            res = self.client.count(index="rm_search_staging", doc_type="m", body=query, request_timeout=36000)
            total_num.append(res['count'])

        return total_num


if __name__ == '__main__':
    counting = CountReleaseMessages()
    print counting.top_relevant(5787)
    print counting.total_relevant(5787)
    # counting.from_csv('/home/nhat.vo/id uk.csv', '/home/nhat.vo/count_id_uk.csv')