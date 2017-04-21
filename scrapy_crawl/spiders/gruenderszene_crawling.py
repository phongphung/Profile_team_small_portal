from scrapy.spiders import BaseSpider
from utils.my_mongo import Mongodb
import logging


class AutoRobot(BaseSpider):
    name = "gruenderszene_crawling"
    allowed_domains = ['www.gruenderszene.de']
    start_urls = []
    mongodb_save = None
    mongodb_read = None
    download_delay = 3
    logger = None

    def init_urls(self):
        self.start_urls = []
        for url in self.mongodb_read.find(select=['url']):
            if url.get('url'):
                self.start_urls.append(url['url'])

        self.logger.info('Crawling url count: %s' % len(self.start_urls))

    def init_mongodb(self):
        self.mongodb_save = Mongodb(db='publisher', col='gruenderszene_investor')
        self.mongodb_read = Mongodb(db='publisher', col='gruenderszene_investor_url')

    def __init__(self, *a, **kw):
        super(AutoRobot, self).__init__(*a, **kw)
        self.logger = logging.getLogger(__name__)
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        pre_name = response.xpath("//h1[@class='profile-name']/text()").extract()
        pre_title = response.xpath("//p[@class='teaser']/text()").extract()
        twitter_url = response.xpath("//*[@id='socials-twitter']/a/@href").extract()
        pre_des = response.xpath('//div[@class="profile-description"]/p[2]/text()').extract()

        profile = dict(
            name=pre_name[0].strip() if pre_name else '',
            title=pre_title[0].strip() if pre_title else '',
            twitter=twitter_url[0].split('/')[-1] if twitter_url else '',
            description=pre_des[0].strip() if pre_des else ''
        )

        self.mongodb_save.insert(profile)
