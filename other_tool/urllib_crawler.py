__author__ = 'sunary'


import urllib
import pandas as pd
import ast
import re


class UrllibCrawler():

    def __init__(self):
        pass

    def freebase_by_text(self):
        self.key = 'AIzaSyAiodi9neqdclj0_K-4-YSU7xlwdk1SDe8'
        fields = ['id', 'name', 'm_id', 'text_id', 'alias', 'lang']
        dataframe = {}
        for f in fields:
            dataframe[f] = []

        pd_file = pd.read_csv('/home/sunary/data_report/150630_org_listed_companies.csv')
        for i in range(0, min(10, len(pd_file['name']))):
            page = urllib.urlopen('https://www.googleapis.com/freebase/v1/search?query=%s&key=%s' % (str((pd_file['name'][i]).replace(' ', '%20')), self.key)).read()
            print page
            dict_page = ast.literal_eval(page)
            if dict_page['status'] == '200 OK':
                for result in dict_page['result']:
                    dict_alias = self.freebase_by_id(result['mid'])
                    try:
                        for value in dict_alias['property']['values']:
                            dataframe['id'].append(pd_file['id'][i])
                            dataframe['name'].append(pd_file['name'][i])
                            dataframe['m_id'].append(result['mid'])
                            dataframe['text_id'].append(result['id'])
                            dataframe['alias'].append(value['text'])
                            dataframe['lang'].append(value['lang'])
                    except:
                        pass

        pd_file = pd.DataFrame(data=dataframe, columns=fields)
        pd_file.to_csv('/home/sunary/data_report/result/freebase_1.csv', index= False)

    def freebase_by_id(self, m_id):
        page = urllib.urlopen('https://www.googleapis.com/freebase/v1/topic%s?filter=/common/topic/alias&key=%s' % (m_id, self.key)).read()
        return ast.literal_eval(page)

    def wikipedia_symbol(self):
        page = urllib.urlopen('https://en.wikipedia.org/wiki/Currency_symbol').read()
        find_all_td = re.findall('<td.*?>(.*?)</td>\s*<td.*?>(.*?)</td>\s*<td.*?>(.*?)</td>', page)

        currency_symbol = []
        for td3 in find_all_td:
            for td in td3:
                if td:
                    find_text = re.findall('>([^>]+?)</a>', td)
                    if find_text:
                        currency_symbol.append(' + '.join(find_text))
                    else:
                        find_text = re.findall('>([^>]+?)</span>', td)
                        if find_text:
                            currency_symbol.append(' + '.join(find_text))
                        else:
                            find_text = re.findall('>([^>]+?)<', td)
                            if find_text:
                                currency_symbol.append(' + '.join(find_text))
                            else:
                                currency_symbol.append(td)
                else:
                    currency_symbol.append('')

        of = open('/home/sunary/data_report/result/symbol.csv', 'w')
        count = 0
        for symbol in currency_symbol:
            count += 1
            of.write(symbol)
            if count == 3:
                of.write('\n')
                count = 0
            else:
                of.write(',')
        of.close()

    def stoxx_ticker(self):
        page = urllib.urlopen('https://www.stoxx.com/web/stoxxcom/component-details?key=477518').read()
        print page
        regex_name = re.match('.+<title>(.+?)</title>.+', page)
        regex_name = regex_name.group(1)[16:] if regex_name else ''
        regex_isin = re.match('.+<td>ISIN\:.*?</td>.*?<td>(.+?)</td>.+', page)
        regex_isin = regex_isin.group(1) if regex_isin else ''
        regex_ric = re.match('.+<td>RIC\:.*?</td>.*?<td>(.+?)</td>.+', page)
        regex_ric = regex_ric.group(1) if regex_ric else ''

        data = {'name': regex_name,
                'isin': regex_isin,
                'ric': regex_ric}
        print data

if __name__ == '__main__':
    urllib_crawler = UrllibCrawler()
    urllib_crawler.stoxx_ticker()