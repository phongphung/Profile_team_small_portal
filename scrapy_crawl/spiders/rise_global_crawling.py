# -*- coding: utf-8 -*-
__author__ = 'sunary'


import re
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "rise_global_crawling"

    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 5

    def init_urls(self):
        self.goto_detail = True
        if self.goto_detail:
            self.start_urls = ['https://www.rise.global/the-uk-economists-power-100/d/2398096/full_screen/all/default/1', 'https://www.rise.global/the-uk-economists-power-100/d/2398096/full_screen/all/default/2']
        else:
            self.start_urls = ['https://www.rise.global/fintech-most-influential-powerlist/d/2397623/full_screen/all/default/1', 'https://www.rise.global/fintech-most-influential-powerlist/d/2397623/full_screen/all/default/2']

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='rise_global', col='twitter2')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        if self.goto_detail:
            hxs = HtmlXPathSelector(response)

            player_detail = hxs.select("//td[@class='player-details']//@href").extract()
            for person in player_detail:
                yield Request(person, callback=self.parse_person)
        else:
            twitter = re.findall('.+"http\://www\.twitter\.com/(.+?)".+', response.body)
            print twitter

    def parse_person(self, response):
        hxs = HtmlXPathSelector(response)
        twitter = re.findall('.+"https\://www\.twitter\.com/(.+?)".+', response.body)

        if twitter:
            self.mongodb_save.insert({'twitter': twitter[0]})