# -*- coding: utf-8 -*-
__author__ = 'sunary'


import subprocess
import json
import pandas as pd
from utils import my_helper


class RuleGenerate():
    '''
    Generator json by gnip format from keywords
    '''
    def __init__(self):
        self.rule = {'rules': []}

    def read_csv(self, file_input):
        self.pd_file = pd.read_csv(file_input)

    def generate(self, file_input, file_output):
        self.read_csv(file_input)

        execute_date = set()

        for i in range(len(self.pd_file['NAME'])):
            if self.pd_file['ADD'][i] == 'yes':
                execute_date.add(self.pd_file['NEW STATUS'][i])
                r = {'value': '', 'tag': 'tag-' + str(i)}
                keyword = self.split_semicolon(self.pd_file['KEYWORDS'][i])
                not_keyword = self.split_semicolon(self.pd_file['NOT'][i])
                contain = self.split_semicolon(self.pd_file['REQUEST'][i])

                if keyword or not_keyword:
                    rule_keyword = ''
                    rule_not_keyword = ''
                    for con in contain:
                        if con.lower() == 'message':
                            print rule_keyword
                            if rule_keyword and keyword:
                                rule_keyword += ' OR '
                            rule_keyword += self.join_keyword(keyword= keyword, prefix= '')

                            if rule_not_keyword and not_keyword:
                                rule_not_keyword += ' OR '
                            rule_not_keyword += self.join_keyword(keyword= not_keyword, prefix= '')
                        elif con.lower() == 'description':
                            if rule_keyword and keyword:
                                rule_keyword += ' OR '
                            rule_keyword += self.join_keyword(keyword= keyword, prefix='bio_contains')

                            if rule_not_keyword and not_keyword:
                                rule_not_keyword += ' OR '
                            rule_not_keyword += self.join_keyword(keyword= not_keyword, prefix='bio_contains')

                    rule_keyword = '(' + rule_keyword + ')' if rule_keyword and rule_not_keyword else rule_keyword
                    rule_not_keyword = 'NOT (' + rule_not_keyword + ')' if rule_not_keyword else rule_not_keyword
                    r['value'] = rule_keyword + (' ' if rule_keyword and rule_not_keyword else '') + rule_not_keyword

                    if len(r['value']) > 2048:
                        return 0, self.pd_file['NAME'][i]
                    if len(r['value']) > 0:
                        self.rule['rules'].append(r)

        fo = open(file_output, 'w+')
        fo.write(json.dumps(self.rule, ensure_ascii= False))
        fo.close()

        return list(execute_date), json.dumps(self.rule, ensure_ascii= False, indent=4, sort_keys=True)

    def split_semicolon(self, keyword):
        keyword = keyword.split('; ') if not my_helper.pandas_null(keyword) else []
        if keyword:
            for i in range(len(keyword)):
                if ';' in keyword[i]:
                    keyword[i] = keyword[i].replace(';', '')
                if '"' in keyword[i]:
                    keyword[i] = keyword[i].replace('"', '')

            i = 0
            while i < len(keyword):
                if not keyword[i]:
                    del keyword[i]
                else:
                    i += 1
        return keyword

    def join_keyword(self, keyword = [], prefix = ''):
        gen_keyword = ['']*len(keyword)
        for i in range(len(keyword)):
            gen_keyword[i] = self.add_double_quotes(keyword[i])

        if prefix:
            for i in range(len(gen_keyword)):
                gen_keyword[i] = prefix + ':' + gen_keyword[i]
        return ' OR '.join(gen_keyword)

    def add_double_quotes(self, keyword):
        list_special = [' ', '.', '!', '%', '&', '_', ':', '+', '-', '?', '#', '@', '<', '=', '>', '(', ')']
        for char in list_special:
            if char in keyword:
                return '"' + keyword + '"'
        return keyword

    def post(self, file_output):
        subprocess.Popen('curl -v -X POST -u anh.le@sentifi.com:s1822013! https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/prod/rules.json -d @gnip.json',
                                cwd = file_output,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True)

    def delete(self, file_output):
        subprocess.Popen('curl -v -X DELETE -u anh.le@sentifi.com:s1822013! https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/prod/rules.json -d @gnip.json',
                                cwd = file_output,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True)

# curl -v -X POST -u anh.le@sentifi.com:s1822013! "https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/prod/rules.json" -d @gnip.json
# curl -v -X DELETE -u anh.le@sentifi.com:s1822013! "https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/prod/rules.json" -d @gnip.json
# curl -v -X POST -u anh.le@sentifi.com:s1822013! "https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/dev/rules.json" -d @gnip.json
# curl -v -X DELETE -u anh.le@sentifi.com:s1822013! "https://api.gnip.com:443/accounts/Sentifi/publishers/twitter/streams/track/dev/rules.json" -d @gnip.json
# mongoexport -h mongo6.ssh.sentifi.com -d sentifi -c tweet -f text -q '{"_id":{$gt:ObjectId("55762c800000000000000000")}, "_source": "gnip"}' --limit 500 --csv -o lastest_messages.csv

if __name__ == '__main__':
    generate = RuleGenerate()
    generate.generate('/home/nhat/data/Publisher Candidates Inventory - GNIP Rules.csv',
                      '/home/nhat/data/result/gnip.json')
    # generate.post('/home/nhat/data/result/gnip.json')