__author__ = 'sunary'


from scrapy import Selector
from scrapy.http import Request
from scrapy.spider import BaseSpider
from utils.my_mongo import Mongodb
import re


class AutoRobot(BaseSpider):
    name = "googlefinance_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    download_delay = 3

    def init_urls(self):
        self.start_urls = []
        mongo = Mongodb(db='gsx', col='name')
        res = mongo.find({}, ['name'])
        for doc in res:
            self.start_urls.append('https://www.google.com/finance?q=%s&ei=L1jpVdHoDMHAuATPiKXoCA' % doc['name'].replace(' ', '+'))

    def init_mongodb(self):
        self.mongodb_save = Mongodb(host='localhost', db='gsx', col='mkt')

    def __init__(self):
        self.init_urls()
        self.init_mongodb()

    def parse(self, response):
        hxs = Selector(response)
        mkt = hxs.xpath('//div[@class="snap-panel"]//td/text()').extract()
        data = {'name': self.get_name(response.url),
                'mkt': self.get_mkt(mkt)}

        self.mongodb_save.insert(data)

    def get_name(self, url):
        match = re.match('.+q=(.+)\&ei=.+', url)
        if match:
            return match.group(1).replace('+', ' ')

        return 'unknow'

    def get_mkt(self, list_td):
        if list_td:
            for i in range(len(list_td) - 1):
                if list_td[i] == 'Mkt cap\n':
                    return list_td[i + 1][:-1]

        return ''
