__author__ = 'sunary'


import pandas as pd
from utils import my_helper
import re


class CombineTwitterProfile():
    '''
    Combine twitter profile csv if there are two rows similar
    '''
    def __init__(self):
        self.file_source = '/home/nhat/data/result/follower_following.csv'
        self.file_sorted = '/home/nhat/data/result/follower_following_sorted.csv'
        self.file_not_duplication = '/home/nhat/data/result/follower_following_sorted_checkdup.csv'

        self.original_fields = my_helper.get_dataframe_columns(self.file_source)

        self.generate_fields = self.original_fields[::]
        self.profile_model = {}
        for i in range(len(self.original_fields)):
            self.profile_model[self.original_fields[i]] = []

    def combine_follower_following(self):
        follower_following = self.profile_model

        self.generate_fields.append('follower')
        self.generate_fields.append('following')

        pd_file = pd.read_csv('/home/sunary/data/follower.csv')
        #copy from file_follower
        for i in range(len(pd_file['id'])):
            for f in range(len(self.original_fields) - 2):
                follower_following[self.generate_fields[f]].append(pd_file[self.original_fields[f]][i])
            follower_following['follower'].append(pd_file['follower'][i])
            follower_following['following'].append('')

        pd_file = pd.read_csv('/home/sunary/data/following.csv')
        #copy from file_following
        for i in range(len(pd_file['id'])):
            for f in range(len(self.original_fields) - 2):
                follower_following[self.generate_fields[f]].append(pd_file[self.original_fields[f]][i])
            follower_following['follower'].append('')
            follower_following['following'].append(pd_file['following'][i])

        pd_file = pd.DataFrame(data=follower_following, index=None, columns=self.generate_fields)
        pd_file.to_csv('/home/sunary/data/follower_following.csv', index= False)

    def sort_by_id(self):
        pd_file = pd.read_csv(self.file_source)
        pd_file = pd_file.sort(['id'], ascending=[1])
        pd_file.to_csv(self.file_sorted, index= False)

    def combine_duplication_profile(self):
        pd_file = pd.read_csv(self.file_sorted)
        distinct_profile = self.profile_model

        id_follower = 12
        id_following = 13

        last_id = '(-*-)'
        for i in range(len(pd_file['id'])):
            if last_id == pd_file['id'][i]:
                if not my_helper.pandas_null(pd_file[self.original_fields[id_follower]][i]):
                    distinct_profile[self.generate_fields[id_follower]][-1] += ('-' + str(int(pd_file[self.original_fields[id_follower]][i])))
                if not my_helper.pandas_null(pd_file[self.original_fields[id_following]][i]):
                    distinct_profile[self.generate_fields[id_following]][-1] += ('-' + str(int(pd_file[self.original_fields[id_following]][i])))
            else:
                last_id = pd_file['id'][i]
                if not my_helper.pandas_null(pd_file['id'][i]):
                    for f in range(len(self.original_fields) - 2):
                        distinct_profile[self.generate_fields[f]].append(pd_file[self.original_fields[f]][i])
                    distinct_profile[self.generate_fields[id_follower]].append(str(int(pd_file[self.original_fields[id_follower]][i])) if not my_helper.pandas_null(pd_file[self.original_fields[id_follower]][i]) else '')
                    distinct_profile[self.generate_fields[id_following]].append(str(int(pd_file[self.original_fields[id_following]][i])) if not my_helper.pandas_null(pd_file[self.original_fields[id_following]][i]) else '')

        pd_file = pd.DataFrame(data=distinct_profile, columns=self.generate_fields)
        pd_file.to_csv(self.file_not_duplication, index= False)

    def filter_profile(self):
        pd_file = pd.read_csv(self.file_source)
        distinct_profile = self.profile_model
        list_keyword = ['investor relation', 'investor-relation', 'ir professional', 'ir specialist','ir director', 'ir services', 'ir consultant','ir analyst', 'ir executive', 'ir team', 'iragency']
        # list_keyword = ['corporate communication', 'corporate comm', 'corp comm']

        for i in range(len(pd_file['id'])):
            if not my_helper.pandas_null(pd_file['description'][i]):
                if any(keyword in str(pd_file['description'][i]).lower() for keyword in list_keyword) or str(pd_file['screen_name'][i]).lower().startswith('ir ') or ' ir ' in str(pd_file['screen_name'][i]).lower():
                    for f in range(len(self.generate_fields)):
                        distinct_profile[self.generate_fields[f]].append(pd_file[self.original_fields[f]][i])

        pd_file = pd.DataFrame(data=distinct_profile, columns=self.generate_fields)
        pd_file.to_csv('/home/nhat/data/result/follower_following_sorted_checkdup_filter_ir.csv', index= False)

if __name__ == '__main__':
    combine_twitter_profile = CombineTwitterProfile()
    # combine_twitter_profile.combine_follower_following()
    # combine_twitter_profile.sort_by_id()
    # combine_twitter_profile.combine_duplication_profile()
    combine_twitter_profile.filter_profile()