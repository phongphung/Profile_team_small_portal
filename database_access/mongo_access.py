__author__ = 'sunary'


from utils.my_mongo import Mongodb
from twitter.twitter_services import TwitterService
import os
from utils import my_text, my_connection
import pandas as pd


class MongoAccess():

    def __init__(self):
        pass

    def export_muckrack(self):
        '''
        check muckrack-user' twitter exist in publisher
        export to csv
        '''
        mongo_muckrack = Mongodb(db='muckrack', col='journalist')

        dataframe = {}
        fields_dataframe = ['name', 'workspace', 'location', 'about', 'twitter', 'twitter_id', 'facebook', 'klout', 'website', 'sentifi_publisher']
        for f in fields_dataframe:
            dataframe[f] = []

        twitter_service = TwitterService('/'.join([os.path.dirname(os.path.abspath(__file__)), '../twitter/twitter_key.yml']))
        con_psql1 = my_connection.psql1_connection()
        cur_psql1 = con_psql1.cursor()

        res = mongo_muckrack.find({}, ['name', 'location', 'workspace', 'about', 'twitter', 'facebook', 'klout', 'website'])
        for doc in res:
            dataframe['name'].append(doc.get('name'))
            dataframe['workspace'].append(doc.get('location'))
            dataframe['location'].append(doc.get('workspace'))
            dataframe['about'].append(doc.get('about'))
            dataframe['twitter'].append(doc.get('twitter'))
            dataframe['facebook'].append(doc.get('facebook'))
            dataframe['klout'].append(doc.get('klout'))
            dataframe['website'].append(doc.get('website'))

            twitter_id = ''
            if doc.get('twitter'):
                twitter_screen_name = my_text.username_from_url(doc.get('twitter'))
                if twitter_screen_name:
                    twitter_id = twitter_service.get_ids_by_screen_names([twitter_screen_name])
                    if twitter_id:
                        twitter_id = twitter_id[0]
                        if twitter_id:
                            cur_psql1.execute('''
                                SELECT True
                                FROM mg_publisher_sns_clean
                                WHERE sns_id = %s
                                    AND sns_name = 'tw'
                            ''', [str(twitter_id)])
                            dataframe['sentifi_publisher'].append(True if cur_psql1.rowcount else False)
                    else:
                        twitter_id = ''

            dataframe['twitter_id'].append(twitter_id)
            if not twitter_id:
                dataframe['sentifi_publisher'].append(False)

        dataframe = pd.DataFrame(data=dataframe, index=None, columns=fields_dataframe)
        dataframe.to_csv('/home/nhat/data/muckrack.csv', index=False)

        cur_psql1.close()

if __name__ == '__main__':
    mongo_access = MongoAccess()
    mongo_access.export_muckrack()