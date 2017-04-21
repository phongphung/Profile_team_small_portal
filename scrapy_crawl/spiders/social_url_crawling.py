# -*- coding: utf-8 -*-
__author__ = 'sunary'


from scrapy.spider import BaseSpider
from utils.my_mongo import Mongodb
import re


class AutoRobot(BaseSpider):
    name = "social_url_crawling"

    def init_urls(self):
        self.start_urls = []
        res = self.mongodb_save.find({}, ['url'])
        for doc in res:
            self.start_urls.append(doc['url'])

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='university', col='info')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        data = {'linkedin': '',
                'facebook': '',
                'twitter': ''}

        linkedin = re.findall('href="([^"]+?linkedin\.com/[^"]+?)"', response.body)
        if len(linkedin) > 0:
            data['linkedin'] = '; '.join(linkedin)

        facebook = re.findall('href="([^"]+?facebook\.com/[^"]+?)"', response.body)
        if len(facebook) > 0:
            data['facebook'] = '; '.join(facebook)

        twitter = re.findall('href="([^"]+?twitter\.com/[^"]+?)"', response.body)
        if len(twitter) > 0:
            data['twitter'] = '; '.join(twitter)

        self.mongodb_save.update({'url': response.url}, data, upsert = False)


