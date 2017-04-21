# -*- coding: utf-8 -*-
__author__ = 'sunary'


import pandas as pd
from utils.my_mongo import Mongodb


class MongoExport():

    def __init__(self):
        self.mongo = Mongodb(host='localhost', db= 'linguee', col= 'hong4')

    def export_linguee(self):
        dictionaries = {'word': [],
                        'meaning': []}
        res = self.mongo.find()

        for doc in res:
            dictionaries['word'].append(doc['word'])
            dictionaries['meaning'].append('"' + doc['meaning'] + '"')

        pd_file = pd.DataFrame(data=dictionaries, columns=['word', 'meaning'])
        pd_file.to_csv('/home/sunary/data_report/result/linguee_hong4.csv', encoding='utf-8', index= False)

        of = open('/home/sunary/data_report/result/linguee_hong4.csv', 'r+')
        alltext = of.read()
        of.close()
        of = open('/home/sunary/data_report/result/linguee_hong4.csv', 'w+')
        alltext = alltext.replace('"""', '"')
        of.write(alltext)
        of.close()

    def export_linguee_many_columns(self):
        dictionaries = {'word': [],
                        'meaning': []}
        res = self.mongo.find()

        for doc in res:
            dictionaries['word'].append(doc['word'])
            dictionaries['meaning'].append('"' + doc['meaning'] + '"')

        pd_file = pd.DataFrame(data=dictionaries, columns=['word', 'meaning'])
        pd_file.to_csv('/home/sunary/data_report/result/linguee_hong3.csv', encoding='utf-8', index= False)

if __name__ == '__main__':
    mongoexport = MongoExport()
    mongoexport.export_linguee()