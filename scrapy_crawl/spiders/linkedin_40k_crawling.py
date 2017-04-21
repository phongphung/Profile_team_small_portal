# -*- coding: utf-8 -*-
__author__ = 'sunary'


from utils import my_helper, my_text
import pandas as pd
from scrapy.http import Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb
import re


class AutoRobot(BaseSpider):
    name = "linkedin_40k_crawling"

    def init_urls(self):
        self.start_urls = []

        self.pd_file = pd.read_csv('/home/sunary/data_report/2015mar24_bloomberg_mapping_40k_all.csv')
        for i in range(len(self.pd_file['name'])):
            if not my_helper.pandas_null(self.pd_file['website'][i]):
                self.start_urls.append(self.pd_file['website'][i])

    def init_request(self):
        return Request(url=self.login_page, callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='linkedin_40k', col='linkedin')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        data = {'name': self.get_company_name(my_text.root_url(response.url)),
                'url': response.url,
                'linkedin': ''}

        match = re.findall('href="([^"]+?linkedin\.com[^"]+?)"', response.body)
        if len(match) > 0:
            data['linkedin'] = '; '.join(match)
            if self.mongodb_save.count({'url': response.url}) <= 0:
                self.mongodb_save.insert(data)

    def get_company_name(self, url):
        for i in range(len(self.pd_file['name'])):
            if not my_helper.pandas_null(self.pd_file['website'][i]) and url in self.pd_file['website'][i]:
                return self.pd_file['name'][i]

        return ''
