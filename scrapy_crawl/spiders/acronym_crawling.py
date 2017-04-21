# -*- coding: utf-8 -*-
__author__ = 'sunary'


import re
import itertools
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "acronym_crawling"

    home_page = 'http://www.acronymfinder.com/'
    filter = ['Information-Technology', 'Military-and-Government', 'Science-and-Medicine', 'Organizations', 'Business', 'Slang']

    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 9


    def init_urls(self):
        self.start_urls = ['http://www.acronymfinder.com/Index--.html']

    def all_acronym(self):
        alphabetical = [chr(c + ord('a')) for c in range(26)]
        for li in itertools.combinations_with_replacement(alphabetical, 2):
            print ''.join(str(c) for c in li)

    def init_request(self):
        pass

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='acronym', col='af')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()
        #self.init_request()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        sort_page = hxs.select("//table[@class='acidx']//@href").extract()

        for href in sort_page:
            for f in self.filter:
                if self.mongodb_save.count({'url': self.home_page[:-1] + href}) <= 0:
                    new_url = self.home_page + f + href
                    yield Request(new_url, callback=self.parse_acronym)

        alphabetical = [chr(c + ord('A')) for c in range(26)]
        for C in alphabetical:
            character_page = hxs.select("//div[@id='p_" + C + "']//@href").extract()
            for href in character_page:
                new_url = self.home_page[:-1] + href
                yield Request(new_url, callback=self.parse_sort_acronym)

    def parse_sort_acronym(self, response):
        hxs = HtmlXPathSelector(response)
        sort_page = hxs.select("//table[@class='acidx']//@href").extract()

        for href in sort_page:
            if self.mongodb_save.count({'url': self.home_page[:-1] + href}) <= 0:
                for f in self.filter:
                    new_url = self.home_page + f + href
                    yield Request(new_url, callback=self.parse_acronym)
        pass

    def parse_acronym(self, response):
        hxs = HtmlXPathSelector(response)
        meaning = hxs.select("//td[@class='result-list__body__meaning']//text()").extract()
        meaning = '|'.join(meaning)
        meaning = meaning if meaning else 'nothing'
        acronym = hxs.select("//h1[@class='acronym__title']/strong//text()").extract()
        url = self.all_url(response.url)

        for f in self.filter:
            if f in response.url:
                data_find = {'acronym': acronym[0]}
                data_insert = {f: meaning, 'url': url}
                self.mongodb_save.update(data_find, data_insert, upsert = True)
                break

    def all_url(self, url):
        filter = re.match('.+acronymfinder.com/(.+?/).+', url)
        url = url.replace(filter.group(1), '')
        return url

    def filter_url(self, url, filter):
        detail = re.match('.+/(.+?.html)', url)
        url = url[:-len(detail.group(1))] + filter + '/' + detail.group(1)
        return url