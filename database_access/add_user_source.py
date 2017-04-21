__author__ = 'sunary'


from utils import my_connection


class AddUserSource():

    def __init__(self):
        self.con_da0 = my_connection.da0_connection()

    def add_from_es(self, update):
        #query form es
        list_source = ['rest', 'stream', 'gnip', 'gnip_by_ir']
        list_source_id = [3, 7, 1, 2]
        client = my_connection.es_connection()
        for src in range(len(list_source)):
            if update:
                query = {
                  "fields": [
                    "publisher.twitterID"
                  ],
                  "query": {
                    "filtered": {
                      "filter": {
                        "bool": {
                          "must": [
                            {
                              "range": {
                                "timestamp": {
                                  "from": "2015-05-07T20:00:00",
                                  "to": "2015-05-05T00:00:00"
                                }
                              }
                            },
                            {
                              "term": {
                                "source": list_source[src]
                              }
                            }
                          ]
                        }
                      }
                    }
                  }
                }
            else:
                query = {
                   "fields": [
                      "publisher.twitterID"
                   ],
                   "query": {
                      "filtered": {
                         "filter": {
                            "term": {
                               "source": list_source[src]
                            }
                         }
                      }
                   }
                }
            res = client.search(index="analytic_tmp",
                                search_type = "count",
                                doc_type="relevant_document",
                                body=query, request_timeout=36000)

            # print res
            num_id = int(res['hits']['total'])
            threshold = 5000
            cur_da0 = self.con_da0.cursor()
            for i in range(num_id/threshold + 1):
                res = client.search(index="analytic_tmp",
                            doc_type="relevant_document",
                            body=query, request_timeout=36000,
                            from_ = i*threshold, size=threshold)

                id_user_source = []
                for j in range(len(res['hits']['hits'])):
                    id_user_source.append(int(res['hits']['hits'][j]['fields']['publisher.twitterID'][0]))

                new_user_source = set()
                cur_da0 = self.con_da0.cursor()
                cur_da0.execute('''
                    SELECT user_id
                    FROM user_source
                    WHERE user_id = ANY(%s)
                        AND source_id = %s
                ''', (id_user_source, list_source_id[src]))

                for item, in cur_da0:
                    new_user_source.add(item)

                new_user_source = set(id_user_source) - new_user_source

                user_source = []
                for id in new_user_source:
                    user_source.append({'id': int(id), 'source': list_source_id[src]})
                cur_da0.executemany('''
                    INSERT INTO twitter.user_source(user_id, source_id)
                    VALUES(%(id)s, %(source)s)
                ''', user_source)
                self.con_da0.commit()

    def add_seed(self):
        list_seed = (2383227282, 2473207808, 1177420700, 571488084, 19576432, 2911535736, 390078949, 17129271, 1491820609, 19050000, 229658053, 65243528, 18878199, 2438234442, 1156715064, 2694777390, 39462076, 2986530574,
                        19581894, 21640980, 1324011068, 301768663, 104712815, 78590498, 26210466, 2533619397, 2497068312, 576196218, 3070265197, 18108170, 245624825, 2371055444, 1533387092, 816348830, 1229055031, 579446082,
                        634611633, 563084750, 152181039, 2248240682, 2898018234, 2481518018, 27652717, 5776022, 2869190566, 235256473, 600864377, 14273050, 85299581, 44144586, 22597568, 86309759, 159503041, 19709133, 18942977,
                        1428038617, 40227292, 54837258, 40197088, 1360335734, 2834511, 20483476, 5494392, 589884194, 5560422, 5715752, 160499861, 37930051, 1081459807, 314459801, 212407067, 2152983800, 2527977343, 57350105,
                        55035997, 15243812, 114508061, 8720562, 22926365, 52421660, 19582266, 19026329, 18631806, 19556781, 17835938, 15865339, 18774524, 23439376, 28314751, 14327439, 18770504, 19339653, 19498323, 102325185,
                        225012752, 2362802845, 16308572, 1328352025, 1093990664, 52530401, 2369090227, 287679097, 30289685, 170936673, 184199830, 42681111, 39928668, 96505990, 175558826, 37018556, 73330888, 145579691, 21909053,
                        25496451, 27624856, 17464780, 22101349, 1457609605, 233998905, 160085181, 233039187, 218167063, 2204511918, 398096178, 70696481, 359907476, 141084074, 14096763, 1547741618, 15327775, 1610278490, 21295652,
                        5776022, 44144586, 600864377, 85299581, 21881699, 159503041, 22597568, 86309759, 14273050, 23922797, 2834511, 15368797, 258994195, 1200474236, 114508061, 38400130, 5494392, 18047862, 19709133, 39922594,
                        18770504, 55191675, 1925582222, 162148752, 14802766)
        cur_da0 = self.con_da0.cursor()

        for id_seed in list_seed:
            try:
                cur_da0.execute('''
                    INSERT INTO user_source(user_id, source_id)
                    VALUES(%s, 4)
                ''', [id_seed])
                self.con_da0.commit()
            except:
                pass

    def add_seed_network(self):
        cur_da0 = self.con_da0.cursor()

        cur_da0.execute('''
            INSERT INTO twitter.user_source(user_id, source_id)
            SELECT user_id, 5
            FROM ((
                SELECT user_id, 5
                FROM follower)
                UNION (
                    SELECT follower_id as user_id, 5
                    FROM follower))
                AS foo
            WHERE NOT EXISTS (
                SELECT NULL
                FROM user_source us
                WHERE us.user_id = foo.user_id
                    AND source_id = 5)
        ''')
        self.con_da0.commit()

    def close_connection(self):
        self.con_da0.close()

if __name__ == '__main__':
    add_user_source = AddUserSource()
    add_user_source.add_seed()
    add_user_source.close_connection()