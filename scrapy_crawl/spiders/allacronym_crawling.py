# -*- coding: utf-8 -*-
__author__ = 'sunary'


import re
import itertools
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "allacronym_crawling"

    home_page = 'http://www.allacronyms.com/'
    filter = ['_technology', '_military', '_government', '_business', '_science', '_organizations', '_medical', '_education', '_internet_slang', '_locations']

    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 1

    list_index = []
    list_acronym = []


    def init_urls(self):
        self.start_urls = []
        self.all_acronym()

    def all_acronym(self):
        alphabetical = [chr(c + ord('a')) for c in range(26)]
        for li in itertools.combinations_with_replacement(alphabetical, 2):
            self.start_urls.append('http://www.allacronyms.com/aa-index-alpha/' + ''.join(str(c) for c in li))

    def init_request(self):
        """This function is called before crawling starts."""
        # return Request(url=self.login_page, callback=self.login)
        pass

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='acronym', col='all')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()
        #self.init_request()

    def parse(self, response):
        #http://www.allacronyms.com/aa-index-alpha/ae
        hxs = HtmlXPathSelector(response)
        acronym_page = hxs.select("//ul[@class='list-unstyled ulindex']//@href").extract()

        is_loop = True
        if self.first_page(response.url):
            if not self.get_index(response.url) in self.list_index:
                yield Request(response.url + '/2', callback=self.parse)
                self.list_index.append(self.get_index(response.url))
                is_loop = False
        else:
            yield Request(self.next_page(response.url), callback=self.parse)
            is_loop = False

        if not is_loop:
            for href in acronym_page:
                for f in self.filter:
                    yield Request(self.home_page + f + href, callback=self.parse_acronym)

    def first_page(self, url):
        return True if not re.match('.+/\d+', url) else False

    def get_index(self, url):
        match = re.match('.+/(\w+)', url)
        return match.group(1).lower()

    def next_page(self, url):
        match = re.match('.+/(\d+)', url)
        return url.replace(match.group(1), str(int(match.group(1)) + 1))

    def parse_acronym(self, response):
        #http://www.allacronyms.com/AE
        hxs = HtmlXPathSelector(response)
        meaning = hxs.select("//div[@class='pairDef']//text()").extract()
        meaning = '|'.join(meaning)
        meaning = meaning if meaning else 'nothing'
        acronym = hxs.select("//div[@class='pairAbb']//text()").extract()
        url = self.all_url(response.url)

        old_meaning = self.mongodb_save.find({'acronym': acronym[0]})
        if old_meaning.count() > 0:
            old_meaning = old_meaning[0]
        else:
            old_meaning = ''

        for f in self.filter:
            if f in response.url:
                if old_meaning and old_meaning.get(f):
                    meaning = old_meaning.get(f) + '|' + meaning
                data_find = {'acronym': acronym[0]}
                data_insert = {f: meaning, 'url': url}
                self.mongodb_save.update(data_find, data_insert, upsert = True)
                break

        if hxs.select("//a[@class='showmore btn btn-block btn-default']"):
            if self.first_page(response.url):
                if not acronym[0] in self.list_acronym:
                    yield Request(response.url + '/2', callback=self.parse_acronym)
                    self.list_acronym.append(acronym[0])
            else:
                yield Request(self.next_page(response.url), callback=self.parse_acronym)

    def all_url(self, url):
        filter = re.match('.+allacronyms.com/(.+?/).+', url)
        url = url.replace(filter.group(1), '')
        return url

    def filter_url(self, url, filter):
        detail = re.match('.+/(.+?.html)', url)
        url = url[:-len(detail.group(1))] + filter + '/' + detail.group(1)
        return url