__author__ = 'sunary'


import pandas as pd
from utils import my_helper, my_text


class EditCSV():
    def __init__(self):
        pass

    def linguee_remove_tag(self):
        pd_file = pd.read_csv('/home/sunary/data_report/result/linguee_hong3.csv')

        for i in range(len(pd_file['meaning'])):
            if not my_helper.pandas_null(pd_file['meaning'][i]):
                new_text = ''
                text = pd_file['meaning'][i].split('; ')
                for t in text:
                    separate = t.split('-')
                    new_text += '-'.join(separate[:-1]) + '; '
                pd_file['meaning'][i] = '"' + new_text + '"'

        pd_file.to_csv('/home/sunary/data_report/result/linguee_hong_remove_tag.csv', index= False)

    def edit_linkedin(self, linkedin):
        linkedin = linkedin.split('; ')
        linkedin.sort()
        new_linkedin = []

        for i in range(len(linkedin)):
            if not linkedin[i].endswith('linkedin.com/') and 'shareArticle' not in linkedin[i]:
                if len(new_linkedin) == 0 or new_linkedin[-1] != linkedin[i]:
                    new_linkedin.append(linkedin[i])

        return '"' + '; '.join(new_linkedin) + '"'
    def valid_linked(self):
        self.pd_file = pd.read_csv('/home/sunary/data_report/2015mar24_bloomberg_mapping_40k_all.csv')
        pd_file_linkedin = pd.read_csv('/home/sunary/data_report/result/linkedin_40k.csv')

        for i in range(len(pd_file_linkedin['linkedin'])):
            if not my_helper.pandas_null(pd_file_linkedin['linkedin'][i]):
                pd_file_linkedin['linkedin'][i] = self.edit_linkedin(pd_file_linkedin['linkedin'][i])
            if my_helper.pandas_null(pd_file_linkedin['name'][i]):
                pd_file_linkedin['name'][i] = self.get_company_name(my_text.root_url(pd_file_linkedin['url'][i]))

        pd_file_linkedin.to_csv('/home/sunary/data_report/result/linkedin_40k_edited.csv', index= False)
        of = open('/home/sunary/data_report/result/linkedin_40k_edited.csv', 'r+')
        alltext = of.read()
        of.close()
        of = open('/home/sunary/data_report/result/linkedin_40k_edited.csv', 'w+')
        alltext = alltext.replace('"""', '"')
        of.write(alltext)
        of.close()

    def get_company_name(self, url):
        url = url.split('.')
        # return url[0]

        for i in range(len(self.pd_file['name'])):
            if not my_helper.pandas_null(self.pd_file['website'][i]) and url in self.pd_file['website'][i]:
                return self.pd_file['name'][i]
        return ''

if __name__ == '__main__':
    edit_csv = EditCSV()
    edit_csv.valid_linked()
