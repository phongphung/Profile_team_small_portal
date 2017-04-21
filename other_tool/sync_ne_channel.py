__author__ = 'sunary'


from datetime import datetime, timedelta
from utils.my_mongo import Mongodb


class ChannelNESync():
    def __init__(self):
        pass

    def sync_all(self):
        mongo_ne = Mongodb(db='worker', col='ChannelNEMatching')

        order = 0
        cursor = mongo_ne.find({'sns_name': 'tw'})
        for doc in cursor:
            update_data = {
                'sns_name': doc.get('sns_name'),
                'sns_id': doc.get('sns_id'),
                'named_entity_id': doc.get('named_entity_id'),
                'accessible': True,
                'available': True,
                'synced_at': datetime.utcnow(),
                'next_crawl_time': datetime.utcnow() + timedelta(seconds= order)
            }
            order += 1
            mongo_ne.update({'_id': doc['_id']},
                                       update_data,
                                       multi=True)

if __name__ == '__main__':
    process = ChannelNESync()
    process.sync_all()