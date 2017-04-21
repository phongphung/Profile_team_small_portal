__author__ = 'sunary'


import pandas as pd
from utils import my_helper


class CombineLinkedinProfile():
    '''
    Combine csv if there are two rows similar
    '''
    def __init__(self):
        self.file_source = '/home/sunary/data_report/result/linkedin_search_by_investor_relation.csv'
        self.file_sorted = '/home/sunary/data_report/result/linkedin_search_by_investor_relation_sort.csv'
        self.file_not_duplication = '/home/sunary/data_report/result/linkedin_search_by_investor_relation_not_dup.csv'

        self.original_fields = my_helper.get_dataframe_columns(self.file_source)

        self.profile_model = {}
        for i in range(len(self.original_fields)):
            self.profile_model[self.original_fields[i]] = []

    def sort_by_url(self):
        pd_file = pd.read_csv(self.file_source)
        pd_file = pd_file.sort(['url'], ascending=[1])
        pd_file.to_csv(self.file_sorted)

    def combine_duplication_profile(self):
        pd_file = pd.read_csv(self.file_sorted)
        distinct_profile = self.profile_model

        last_url = '(-*-)'
        for i in range(len(pd_file['url'])):
            if len(pd_file['url'][i]) > 4 and last_url == pd_file['url'][i]:
                pass
            else:
                for f in range(len(self.original_fields)):
                    distinct_profile[self.original_fields[f]].append(pd_file[self.original_fields[f]][i])
                last_url = pd_file['url'][i]

        pd_file = pd.DataFrame(data=distinct_profile, index=None, columns=self.original_fields)
        pd_file.to_csv(self.file_not_duplication, index= False)

if __name__ == '__main__':
    combine_linkedin_profile = CombineLinkedinProfile()
    combine_linkedin_profile.sort_by_url()
    combine_linkedin_profile.combine_duplication_profile()