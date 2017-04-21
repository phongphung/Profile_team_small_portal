# -*- coding: utf-8 -*-
__author__ = 'sunary'


from scrapy.spider import BaseSpider
from scrapy import Selector
from utils.my_mongo import Mongodb
import string


class AutoRobot(BaseSpider):
    name = "londonstockexchange_crawling"

    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 5

    def init_urls(self):
        self.name = ['350', 'smallcap', 'aim', 'allshare']
        self.base_url = ['http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=NMX',
                    'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-techmarkindices-constituents.html?index=SMX',
                    'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=AXX',
                    'http://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=ASX']
        length = [18, 15, 42, 33]

        self.start_urls = []
        for i in range(len(self.name)):
            self.start_urls.append(self.base_url[i])
            for j in range(2, length[i] + 1):
                self.start_urls.append(self.base_url[i] + '&page=%s' % j)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='londonstockexchange', col='all3')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        hxs = Selector(response)

        exchange_detail = hxs.xpath("//div[@id='pi-colonna1-display']//table//tr//td//text()").extract()
        end_row1 = [u'\r\n\xa0 ', u'\r\n', u' ', u'\r\n', u' ']
        end_row2 = [ u' ', u'\r\n', u' ', u'\r\n', u' ']

        self.mongodb_save.update({'code': exchange_detail[0]},
                                                    {'code': exchange_detail[0],
                                                              'name': exchange_detail[2],
                                                              self.get_name_field(response.url): True},
                                                    upsert=True)
        i = 3
        while i < len(exchange_detail) - (len(end_row1) + 2):
            if (exchange_detail[i:i + len(end_row1)] == end_row1 or exchange_detail[i:i + len(end_row1)] == end_row2) and self.startswith_uppercase(exchange_detail[i + len(end_row1)]):
                self.mongodb_save.update({'code': exchange_detail[i + len(end_row1)]},
                                                    {'code': exchange_detail[i + len(end_row1)],
                                                              'name': exchange_detail[i + len(end_row1) + 2],
                                                              self.get_name_field(response.url): True},
                                                    upsert=True)
                i += len(end_row1) + 2
            else:
                i += 1

    def startswith_uppercase(self, word):
        start_char = string.digits + string.uppercase
        for ch in start_char:
            if word.startswith(ch):
                return True
        return False

    def get_name_field(self, url):
        for i in range(len(self.name)):
            if self.base_url[i] in url:
                return self.name[i]
        return 'xxx'