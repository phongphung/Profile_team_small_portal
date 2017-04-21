__author__ = 'sunary'


from utils import my_helper, my_connection


class PublisherStatistics():

    def __init__(self):
        self.publisher_payload_fields = ['status', 'privateTrader', 'oldCategories', 'description', 'countryCode', 'image', 'address', 'active', 'categories', 'releasedAt', 'city', 'tracking', 'displayName', 'name', 'language', 'categoryId', 'priority', 'person', 'score', 'newsAggregator', 'parentId', 'organization', 'email', 'extras', 'itemId']

    def process(self):
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()
        last_object_id = ''

        self.counter = {}
        self.group = {}

        while True:
            cur_psql1.execute('''
                SELECT object_id, object_payload
                FROM mongo.mg_publisher
                WHERE object_id > %s
                ORDER BY object_id
                LIMIT 10000
            ''', [last_object_id])

            if cur_psql1.rowcount:
                for doc in cur_psql1:
                    if doc[0] > last_object_id:
                        last_object_id = doc[0]

                    status_has_value = [my_helper.not_null(doc[1].get(payload_field)) for payload_field in self.publisher_payload_fields]
                    self.statistics_field(status_has_value)
                    self.statistics_group(status_has_value)
            else:
                break

        con_psql1.close()

    def statistics_field(self, status_has_value):
        self.counter['total'] = self.counter.get('total', 0) + 1

        for i in range(len(status_has_value)):
            if status_has_value[i]:
                self.counter[self.publisher_payload_fields[i]] = self.counter.get(self.publisher_payload_fields[i], 0) + 1

    def statistics_group(self, status_has_value):
        to_key = ['1' if status_has_value[i] else '0' for i in range(len(status_has_value))]
        to_key = ''.join(to_key)
        self.group[to_key] = self.group.get(to_key, 0) + 1

    def export(self, path_dest):
        fo = open(path_dest + 'counter.csv', 'w')
        fo.write('fields,number\n')
        for key, value in self.counter.iteritems():
            fo.write(key + ',' + str(value) + '\n')
        fo.close()

        fo = open(path_dest + 'group.csv', 'w')
        for i in range(len(self.publisher_payload_fields)):
            fo.write(self.publisher_payload_fields[i] + ',')
        fo.write('number\n')

        for key, value in self.group.iteritems():
            for status in key:
                if status == '1':
                    fo.write('X,')
                else:
                    fo.write(',')
            fo.write(str(value) + '\n')

        fo.close()


if __name__ == '__main__':
    statistics = PublisherStatistics()
    statistics.process()
    statistics.export('/home/nhat.vo/')

