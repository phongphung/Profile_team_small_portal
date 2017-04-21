__author__ = 'sunary'


import pandas as pd
import yaml
from utils import my_helper, my_text, my_connection


class WrongLegalnameGenerate:
    def __init__(self):
        self.removed_company_type = set()
        self.abbreviation = {}

    def get_company_type(self):
        con_psql1 = my_connection.ellytran_connection()
        cur_psql1 = con_psql1.cursor()
        cur_psql1.execute('''
            SELECT type_name, type_abbreviation
            FROM company_type
            WHERE status = 1
        ''')
        con_psql1.commit()

        for doc in cur_psql1:
            try:
                self.removed_company_type.add(my_text.normalize(doc[0]))
                self.removed_company_type |= set([my_text.normalize(w) for w in doc[1]])
            except:
                pass

        con_psql1.close()

    def get_abbreviation(self):
        with open('keywords/abbreviation.yml', 'r') as fo:
            self.abbreviation = yaml.load(fo)
            for key, value in self.abbreviation.iteritems():
                self.abbreviation[key] = [my_text.normalize(x) for x in value]

            self.abbreviation['&'] = ['and']

    def process(self, file_source, file_dest):
        self.get_company_type()
        self.get_abbreviation()

        original_fields = my_helper.get_dataframe_columns(file_source)
        generate_fields = original_fields[::]
        generate_fields.append('__not_match' if '_not_match' in generate_fields else '_not_match')
        generate_fields.append('__less' if '_less' in generate_fields else '_less')
        generate_fields.append('__company_type' if '_company_type' in generate_fields else '_company_type')

        original_dataframe = pd.read_csv(file_source)
        generate_dataframe = {}
        for field in generate_fields:
            generate_dataframe[field] = []

        for i in range(len(original_dataframe[original_fields[0]])):
            name = str(original_dataframe['name'][i])
            legalname = str(original_dataframe['legalname'][i])

            for j in range(len(original_fields)):
                generate_dataframe[generate_fields[j]].append(my_helper.except_pandas_value(original_dataframe[original_fields[j]][i]))

            new_name = self.de_abbreviation(name)
            new_legalname = self.de_abbreviation(legalname)
            generate_dataframe[generate_fields[len(original_fields)]].append('match' if self.keywords_match_legalname(name, legalname) or self.keywords_match_legalname(new_name, new_legalname) else 'not')
            compare = 'equal'
            if len(legalname.split(' ')) < len(name.split(' ')):
                compare = 'less'
            elif len(legalname.split(' ')) > len(name.split(' ')):
                compare = 'greater'
            generate_dataframe[generate_fields[len(original_fields) + 1]].append(compare)
            generate_dataframe[generate_fields[len(original_fields) + 2]].append('yes' if any([my_text.normalize(x) in self.removed_company_type for x in my_text.split_text(new_legalname, min_rate=1)]) else 'no')

        original_dataframe = pd.DataFrame(data=my_helper.dataframe_same_length(generate_dataframe, generate_fields), index=None, columns=generate_fields)
        original_dataframe.to_csv(file_dest, index=False)

    def de_abbreviation(self, keyword):
        keyword = my_text.split_text(keyword, min_rate=1)
        for i in range(len(keyword)):
            for key, value in self.abbreviation.iteritems():
                if my_text.normalize(keyword[i], '&') in value:
                    keyword[i] = key

        return ' '.join(keyword)

    def startswith_condition(self, str_contain, str_start):
        '''
        Examples:
            >>> self.startswith_condition('Communication', 'Com')
            True
            >>> self.startswith_condition('F&C', 'F')
            False
        '''
        if str_contain.startswith(str_start):
            if len(str_contain) == len(str_start) or my_text.normalize(str_contain[len(str_start)]):
                return True

        return False

    def keywords_match_legalname(self, keyword, legal_name):

        if my_text.normalize(keyword, '&') == my_text.normalize(legal_name, '&'):
            return True

        legal_name_cutting = my_text.split_text(legal_name, min_rate=1)
        legal_name_cutting = filter(lambda x: my_text.normalize(x, '&'), legal_name_cutting)
        legal_name_cutting = map(lambda x: my_text.normalize(x, '&'), legal_name_cutting)

        keyword_cutting = my_text.split_text(keyword, min_rate=1)
        keyword_cutting = filter(lambda x: my_text.normalize(x, '&'), keyword_cutting)
        keyword_cutting = map(lambda x: my_text.normalize(x, '&'), keyword_cutting)
        len_keyword = len(keyword_cutting)

        if (len_keyword <= len(legal_name_cutting) and all([self.startswith_condition(legal_name_cutting[i], keyword_cutting[i]) for i in range(len_keyword)])) or\
                ''.join(legal_name_cutting).startswith(''.join(keyword_cutting)):
            return True

        return False


if __name__ == '__main__':
    wrong_legalname_generate = WrongLegalnameGenerate()
    print wrong_legalname_generate.keywords_match_legalname('Co.Don AG', 'CO.DON')
    # wrong_legalname_generate.process('/home/nhat.vo/legalname_Screenname_wrong.csv', '/home/nhat.vo/wrong_legalname.csv')