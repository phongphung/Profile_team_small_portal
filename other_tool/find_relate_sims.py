__author__ = 'sunary'


class FindRelateSims(object):

    def __init__(self, es_client, index_name, doc_type):
        self.index_name = index_name
        self.doc_type = doc_type
        self.es_client = es_client

    def process(self, id_msgs):
        doc_ids = list(self.search(set(id_msgs)))
        return sorted(doc_ids)

    def search(self, doc_ids):
        # Search dup_id
        found = False
        new_dup_ids = self.search_by_field('dup_id', '_id', doc_ids)
        if new_dup_ids:
            found = True
            for item in new_dup_ids:
                doc_ids.add(item)

        new_dup_ids = self.search_by_field('_id', 'dup_id', doc_ids)
        if new_dup_ids:
            found = True
            for item in new_dup_ids:
                doc_ids.add(item)

        if found:
            return self.search(doc_ids)

        return doc_ids

    def search_by_field(self, query_field_name, select_field_name, doc_ids):
        new_ids = set()

        query = {
            "fields": [
                select_field_name,
                query_field_name
            ],
            "query": {
                "filtered": {
                    "filter": {
                        "terms": {
                            query_field_name: list(doc_ids)
                        }
                    }
                }
            },
            "size": 10000
        }

        res = self.es_client.search(index=self.index_name,
                                    doc_type=self.doc_type,
                                    body=query,
                                    request_timeout=36000)
        res = res['hits']['hits']

        if res:
            for doc in res:
                for field_name in [query_field_name, select_field_name]:
                    if isinstance(doc['fields'][field_name], list):
                        tmp = doc['fields'][field_name]
                    else:
                        tmp = [doc['fields'][field_name]]
                    for item in tmp:
                        if item not in doc_ids:
                            new_ids.add(str(item))

        return new_ids

if __name__ == '__main__':
    find_sims_id = FindRelateSims()
    find_sims_id.process(['55efe5938d9c303829f7c8a5'])