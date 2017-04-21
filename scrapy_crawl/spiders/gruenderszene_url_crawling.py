from scrapy.spiders import BaseSpider
from utils.my_mongo import Mongodb
import string


class AutoRobot(BaseSpider):
    name = "gruenderszene_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    download_delay = 3

    def init_urls(self):
        self.start_urls = []
        # for l in list(string.ascii_lowercase):
        #     self.start_urls.append('http://www.gruenderszene.de/datenbank/koepfe/found/%s' % l)

        for l in list(string.ascii_lowercase):
            self.start_urls.append('http://www.gruenderszene.de/datenbank/investoren/found/%s' % l)

    def init_mongodb(self):
        self.mongodb_save = Mongodb(db='publisher', col='gruenderszene_investor_url')

    def __init__(self, *a, **kw):
        super(AutoRobot, self).__init__(*a, **kw)
        self.init_urls()
        self.init_mongodb()

    def parse(self, response):
        urls = response.xpath('//*[@id="startlist-wrapper"]/ul/li/h3/a/@href').extract()
        for url in urls:
            self.mongodb_save.insert({'url': url})
