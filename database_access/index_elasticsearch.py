__author__ = 'sunary'


from bson import ObjectId
from datetime import datetime
from elasticsearch.helpers import bulk
from utils import my_helper, my_connection


class ElasticSearchBulkIndex():

    def __init__(self, client, index_name, type_name, doc_id_field_name):
        self.client = client
        self.index = index_name
        self.type = type_name
        self.doc_id_field_name = doc_id_field_name

    def index_docs(self, docs):
        '''
        Index docs in elasticsearch

        Args:
            docs: documents need index
        Return:
            docs: return docs were indexed
        '''
        actions = []
        for doc in docs:
            my_helper.bson2json(doc)

            actions.append(dict(
                _index=self.index,
                _type=self.type,
                _id=str(doc[self.doc_id_field_name]),
                _source=doc
            ))

        bulk(self.client, actions)

        return docs

    def refresh(self):
        self.client.indices.refresh(index = self.index)

class IndexElasticSearch():

    def __init__(self):
        pass

    def index_candidate_data(self):
        '''
        Read tw_user from da0 and index to elasticsearch
        '''
        con_da0 = my_connection.da0_connection()
        cur_da0 = con_da0.cursor()
        es_index = ElasticSearchBulkIndex(my_helper.get_es_connection(), 'candidate_publisher', 'twitter', '_id')

        last_created_at = datetime(2000, 1, 1)
        threshold = 50000
        while True:
            docs_index = []
            cur_da0.execute('''
                SELECT user_id, created_at, payload
                FROM twitter.tw_user
                WHERE created_at > %s
                ORDER BY created_at
                LIMIT %s
            ''', [last_created_at, threshold])

            if cur_da0.rowcount:
                for doc in cur_da0:
                    add_field = {'_id': doc[0], 'timestamp': str(ObjectId.from_datetime(doc[1]))}
                    last_created_at = doc[1]

                    cur_iso_da0 = con_da0.cursor()
                    cur_iso_da0.execute('''
                        SELECT isocode
                        FROM twitter.user_isocode
                        WHERE user_id = %s
                    ''', [doc[0]])

                    if cur_iso_da0.rowcount:
                        add_field['isocode'] = cur_iso_da0.next()[0]

                    if doc[2]:
                        doc[2].update(add_field)
                        docs_index.append(doc[2])
                    else:
                        docs_index.append(add_field)

                es_index.index_docs(docs_index)
            else:
                break

        con_da0.close()

if __name__ == '__main__':
    index_elastcisearch = IndexElasticSearch()
    index_elastcisearch.index_candidate_data()

