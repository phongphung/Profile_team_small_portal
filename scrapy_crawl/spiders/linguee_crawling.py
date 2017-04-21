# -*- coding: utf-8 -*-
__author__ = 'sunary'


import re
import pandas as pd
from random import randint
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "linguee_crawling"

    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 10

    def init_urls(self):
        self.many_columns = False
        self.keep_tag = False

        self.start_urls = []
        pd_file = pd.read_csv('/home/sunary/data_report/CompanyL_Crawler.csv')
        for word in pd_file['English']:
            self.start_urls.append(
                'http://www.linguee.com/english-german/search?source=auto&query=' + str(word).replace(' ', '+'))

    def init_request(self):
        """This function is called before crawling starts."""
        # return Request(url=self.login_page, callback=self.login)
        pass

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='linguee', col='hong4')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()
        # self.init_request()

    def parse(self, response):
        self.DOWNLOAD_DELAY = 8 + randint(5, 10)

        hxs = HtmlXPathSelector(response)
        need_tran = hxs.select("//title//text()").extract()
        tag_trans = hxs.select("//span[@class='tag_trans']").extract()

        if self.many_columns:
            data = {'word': '',
                    'n': '',
                    'nt': '',
                    'pl': '',
                    'v': '',
                    'adj': '',
                    'adj-e': '',
                    'adj-er': '',
                    'adj-es': '',
                    'adj-en': '',
                    'adj-em': '',
                    'other': ''}

            data['word'] = need_tran[0].split(' - ')[0]
            for tag in tag_trans:
                find_all = re.findall(u'>[^<]+<', tag)
                tag_type = ''
                for t in find_all[1:]:
                    if t != '> <':
                        tag_type = t[1:-1]
                        break

                if tag_type == 'n':
                    data['n'] += find_all[0][1:-1] + '; '
                elif tag_type == 'nt':
                    data['nt'] += find_all[0][1:-1] + '; '
                elif tag_type == 'pl':
                    data['pl'] += find_all[0][1:-1] + '; '
                elif tag_type == 'v':
                    data['v'] += find_all[0][1:-1] + '; '
                elif tag_type == 'adj':
                    data['adj'] += find_all[0][1:-1] + '; '
                    data['adj-e'] += find_all[0][1:-1] + 'e; '
                    data['adj-er'] += find_all[0][1:-1] + 'er; '
                    data['adj-es'] += find_all[0][1:-1] + 'es; '
                    data['adj-en'] += find_all[0][1:-1] + 'en; '
                    data['adj-em'] += find_all[0][1:-1] + 'em; '
                else:
                    data['other'] += find_all[0][1:-1]
                    data['other'] += '-' + tag_type + '; '
        else:
            data = {'word': '',
                    'meaning': ''}

            data['word'] = need_tran[0].split(' - ')[0]
            for tag in tag_trans:
                find_all = re.findall(u'>[^<]+<', tag)
                tag_type = ''
                for t in find_all[1:]:
                    if t != '> <':
                        tag_type = t[1:-1]
                        break
                data['meaning'] += find_all[0][1:-1]
                if self.keep_tag:
                    data['meaning'] += '-' + tag_type + '; '
                else:
                    data['meaning'] += '; '

        if self.mongodb_save.count({'word': data['word']}) <= 0:
            self.mongodb_save.insert(data)