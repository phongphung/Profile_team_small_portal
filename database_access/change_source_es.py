__author__ = 'sunary'


from utils import my_connection


class ChangeESSource():

    def __init__(self):
        pass

    def edit_source(self):
        client = my_connection.es_connection()
        query_count = {
            "query": {
                "filtered": {
                   "filter": {
                       "range": {
                          "timestamp": {
                             "from": "2015-04-21T10:30:00"
                             # "from": "2015-04-23T09:25:00"
                          }
                       }
                   }
                }
            }
        }

        query_get_id = {
            "query": {
                "filtered": {
                   "filter": {
                       "range": {
                          "timestamp": {
                             "from": "2015-04-21T10:30:00"
                          }
                       }
                   }
                }
            },
            "fields": ["source"]
        }

        query_by_id = {
            "query": {
                "ids": {
                    "values": ["5535d54875cb1d18ca5d913c"]
                }
            }

        }

        query_update = {
            "doc": {
                "source":"gnip_by_IR"
            }
        }

        res = client.count(index="analytic_tmp",
                       doc_type="relevant_document",
                       body=query_count,
                       request_timeout=36000)

        num_update = res['count']
        print num_update

        threshold = 5000
        for i in range(num_update/threshold + 1):
            res = client.search(index="analytic_tmp",
                        doc_type="relevant_document",
                        body=query_get_id,
                        request_timeout=36000,
                        from_ = i*threshold, size=threshold)

            document_ids = res['hits']['hits']
            for j in range(len(document_ids)):
                client.update(index="analytic_tmp",
                       doc_type="relevant_document",id=document_ids[j]['_id'], body=query_update, request_timeout=36000)
                # print document_ids[j]['_id']

if __name__ == '__main__':
    change_source = ChangeESSource()
    change_source.edit_source()
