__author__ = 'sunary'


from scrapy.selector import HtmlXPathSelector
from scrapy.spider import BaseSpider
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "football_club_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    download_delay = 3

    def init_urls(self):
        self.start_urls = []
        for i in range(1, 45):
            self.start_urls.append('http://footballdatabase.com/ranking/world/%s' % (i))

    def init_mongodb(self):
        self.mongodb_save = Mongodb(db='fc', col='fc')

    def __init__(self):
        self.init_urls()
        self.init_mongodb()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        club_name = hxs.select("//div[@class='limittext']/text()").extract()
        country = hxs.select("//a[@class='sm_logo-name']/text()").extract()

        for i in range(len(club_name)):
            try:
                data = {'club': club_name[i],
                        'country': country[i]}
                self.mongodb_save.insert(data)
            except:
                pass
