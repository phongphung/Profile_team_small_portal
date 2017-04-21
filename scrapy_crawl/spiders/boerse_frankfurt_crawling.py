__author__ = 'sunary'


from scrapy import Selector
from scrapy.http import Request
from scrapy.spider import BaseSpider
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "boerse_frankfurt_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    download_delay = 2

    def init_urls(self):
        self.name = ['tecdax', 'sdax', 'mdax', 'cdax', 'dax']

        self.start_urls = ['http://www.boerse-frankfurt.de/en/equities/indices/dax+DE0008469008/constituents',
                           'http://www.boerse-frankfurt.de/en/equities/indices/tecdax+DE0007203275/constituents',
                           'http://www.boerse-frankfurt.de/en/equities/indices/mdax+DE0008467416/constituents',
                           'http://www.boerse-frankfurt.de/en/equities/indices/sdax+DE0009653386/constituents',
                           'http://www.boerse-frankfurt.de/en/equities/indices/cdax+performance+DE0008469602/constituents']

        self.start_urls = self.start_urls[4:5]

    def init_mongodb(self):
        self.mongodb_save = Mongodb(host='localhost', db='bf', col='tk')

    def __init__(self):
        self.init_urls()
        self.init_mongodb()

    def parse(self, response):
        hxs = Selector(response)
        company_pages = hxs.xpath("//div[@class='fullbox list_component']//@href").extract()

        if company_pages:
            for page in company_pages:
                if 'equities' in page:
                    # self.mongodb_save.insert({'url': 'http://www.boerse-frankfurt.de' + page})
                    yield Request('http://www.boerse-frankfurt.de' + page, callback=self.parse_ticker)

    def parse_ticker(self, response):
        hxs = Selector(response)
        info = hxs.xpath("//div[@class='fullbox']/div[@class='info']//text()").extract()

        field_name = self.get_name_field(response.request.headers.get('Referer', 'xxx'))
        data = {'name': info[1],
              'ticker': info[3].split(', ')[-1],
              field_name: True}
        if field_name in ['dax', 'mdax', 'tecdax']:
            data['hdax'] = True

        self.mongodb_save.insert(data)

    def get_name_field(self, url):
        for i in range(len(self.name)):
            if self.name[i] in url:
                return self.name[i]
        return 'xxx'