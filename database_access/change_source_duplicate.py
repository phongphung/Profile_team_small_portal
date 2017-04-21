__author__ = 'sunary'


from datetime import datetime
from bson import ObjectId
from queue.my_queue import Queue
from utils import my_connection
from utils.my_mongo import Mongodb


class ChangeSourceDuplicate():

    def __init__(self):
        self.queue = Queue({
            'uri':['amqp://sentifi:onTheStage@node-staging-01.ireland.sentifi.internal'],
            'name': 'StorageToElasticsearchProcessor_StagingEsRelevantDocument'
        })

        self.mongo_raw = Mongodb(host='dev.ssh.sentifi.com',
                                 port=27017,
                                 db='analytic_test',
                                 col='relevant_document')
        self.elasticsearch = my_connection.es_connection()

    def scan_id(self):
        from_id = ObjectId.from_datetime(datetime(2015, 1, 1))
        to_id = ObjectId.from_datetime(datetime(2015, 7, 17))
        while True:
            cursor = self.mongo_raw.find({'_id': {'$gt': from_id, '$lt': to_id}, 'dup_id': {'$exists': 1}}, ['_id'])

            for doc in cursor:
                change_dup_ids = self.search_by_field('_id', 'dup_id', set([doc['_id']]), set([doc['_id']]))
                if change_dup_ids:
                    self.change_dup_id(change_dup_ids)
                    # self.post_to_queue(change_dup_ids)

            if not any(True for _ in cursor):
                break

    def search_by_field(self, query_field_name, select_field_name, storage_ids, new_ids):
        '''
        Search _id or dup_id in elasticsearch

        Args:
            query_field_name: field query
            select_field_name: field select
            storage_ids: all result was storage
            new_ids: result just created
        Return:
            storage_ids: _id or dup_id were found
        '''
        query = {
            "fields": [
                select_field_name
            ],
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            query_field_name: list(new_ids)
                        }
                    }
                }
            }
        }
        res = self.es_client.search(index=self.index_name,
                                    doc_type=self.doc_type,
                                    body=query,
                                    request_timeout=36000)
        res = res['hits']['hits']

        if res:
            new_ids = set()
            for doc in res:
                doc = doc['fields'].get(select_field_name)
                if doc:
                    if isinstance(doc, basestring):
                        if doc not in storage_ids:
                            new_ids.add(doc)
                    else:
                        if len(doc) > 0 and doc[0] not in storage_ids:
                            new_ids.add(doc[0])

            if new_ids:
                storage_ids |= new_ids
                return self.search_by_field(select_field_name, query_field_name, storage_ids, new_ids)

        return storage_ids

    def change_dup_id(self, msg_ids):
        '''
        Change dup_id of _id doc by minimum input msg_ids

        Args:
            msg_ids: list of messages id
        '''
        source_sim = min(msg_ids)
        for msg_id in msg_ids:
            self.mongo_raw.update({'_id': ObjectId(msg_id)},
                                 {'dup_id': source_sim, 'is_sim': (True if msg_id != source_sim else False)},
                                 multi=True)

    def post_to_queue(self, msg_ids):
        for msg_id in msg_ids:
            try:
                self.queue.post(msg_id)
            except:
                pass

if __name__ == '__main__':
    change_source = ChangeSourceDuplicate()
    change_source.scan_id()
