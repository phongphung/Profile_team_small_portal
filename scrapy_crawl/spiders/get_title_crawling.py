__author__ = 'sunary'


import pandas as pd
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils import my_helper
import re


class AutoRobot(BaseSpider):
    name = "get_title_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 1

    def init_urls(self):
        pd_file = pd.read_csv('/home/sunary/data_report/result/linkedin_40k_edited.csv')
        self.start_urls = []
        for i in range(len(pd_file['name'])):
            if my_helper.pandas_null(pd_file['name'][i]):
                self.start_urls.append(pd_file['url'][i])

    def __init__(self, *arg):
        self.init_urls()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        title = ''.join(hxs.select("//title/text()").extract())
        title = title.replace('\n', '')
        title = title.replace('Home Page ', '')
        title = title.replace('Home ', '')
        match = re.match('.*?([a-zA-Z].+[a-zA-Z]).*?', title)
        title = match.group(1) if match else title
        fo = open('list_title.txt', 'a')
        fo.write(title + '-|-' + response.url + '\n')
        fo.close()
