__author__ = 'sunary'


import pandas as pd
import re
import urllib
from utils import my_helper
from scrapy.http import Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "google_finance_crawling"
    # allowed_domains = []
    start_urls = []
    paths = []
    mongodb = None
    mongodb_save = None
    list_id_corp = []
    list_website_corps = []

    dict_id_url = []
    # rules = (
    # Rule(SgmlLinkExtractor(allow=r'-\w+.html$'),
    #      callback='parse_item', follow=True),
    # )

    def init_urls(self):
        #self.login_page = 'http://www.domain.com/login'
        file_list_ids = pd.read_csv('/home/sunary/data_report/crawl_and_check.csv')
        self.list_id_corp = file_list_ids['id']
        self.list_name_corps = file_list_ids['name']
        self.list_website_corps = file_list_ids['www']

        for legalname_id in range(len(file_list_ids['legalname'])):
            if not my_helper.pandas_null(file_list_ids['legalname'][legalname_id]):
                self.list_name_corps[legalname_id] = file_list_ids['legalname'][legalname_id]

        self.start_urls = []
        for name in self.list_name_corps:
            self.start_urls.append(
                'https://www.google.com/finance?noIL=1&q=' + str(name) + '&ei=Gr85VamrCaX-uQTK1YGgBA')
            # self.start_urls = ['google.com/finance?q=NASDAQ%3APIH']
            #scrapy crawl google_finance_crawling -o items.csv -t csv
            #mongoexport --db reuters_data --collection corps --csv --fields Name,Symbol,Exchange --out corps.csv

    def init_request(self):
        """This function is called before crawling starts."""
        return Request(url=self.login_page, callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='google_finance_data', col='all_corps')

    def __init__(self, *arg):
        self.init_urls()
        #self.init_request()
        self.init_mongodb()

    def parse(self, response):
        id_corp = self.id_of_link(response.url)
        hxs = HtmlXPathSelector(response)
        list_urls = hxs.select('//td[@class="localName nwp"]//@href').extract()
        i = 0
        while i < len(list_urls) and i < 4:
            # print 'https://www.google.com' + list_urls[i]
            self.dict_id_url.append([self.list_id_corp[id_corp], 'https://www.google.com' + list_urls[i]])
            yield Request('https://www.google.com' + list_urls[i], callback=self.parse_detail)
            i += 1

    def parse_detail(self, response):
        try:
            id_corp = self.get_id_map_link(response.url)

            hxs = HtmlXPathSelector(response)
            name_field = hxs.select('//div[@id="companyheader"]/div[@class="g-unit g-first"]//text()').extract()
            # symbol_field = hxs.select('//div[@class="elastic"]/div[@class="appbar-center"]/div[@class="appbar-snippet-secondary"]//text()').extract()

            address_field = hxs.select('//div[@class="g-c"]/div[@class="sfe-section"]//text()').extract()
            address = [element for element in address_field if '\n' not in element and ' - ' not in element]
            check_have_phone = False
            for id_add in range(len(address)):
                if re.match('.*[0-9].*', address[id_add]):
                    check_have_phone = True
                    break
            for id_add in range(len(address)):
                if 'Screen stocks' in address[id_add]:
                    address = address[id_add + 1:]
                if 'Map' in address[id_add]:
                    del address[id_add]
                    break
            while check_have_phone:
                if not re.match('.*[0-9].*', address[-1]):
                    address = address[:-1]
                else:
                    break

            website_field = hxs.select(
                '//div[@class="g-c"]/div[@class="sfe-section"]/div[@class="item"]//text()').extract()
            description_field = hxs.select('//div[@class="sfe-section"]/div[@class="companySummary"]//text()').extract()

            if len(name_field) < 2:
                return

            Name = name_field[0]
            regex = re.search('.*\(Public, (.*)\).*', name_field[1])
            if not regex:
                return
            Symbol = regex.group(1)
            Address = " ".join(address)
            Website = website_field[1][1:-1] if len(website_field) > 1 else ''
            Description = description_field[0][1:-1] if len(website_field) > 0 else ''

            condition = {'Id': self.list_id_corp[id_corp],
                         'Symbol': Symbol,
                         'Name': Name}
            data = {'Id': self.list_id_corp[id_corp],
                    'Name': Name,
                    'Symbol': Symbol,
                    'Website': Website,
                    'Description': Description,
                    'Address': Address}

            if self.mongodb_save.count(condition) <= 0:
                self.mongodb_save.insert(data)
        except:
            pass

    def id_of_link(self, encode_url):
        encode_url = urllib.unquote_plus(encode_url)
        for id_url in range(len(self.start_urls)):
            if encode_url == self.start_urls[id_url]:
                return id_url
        return ''

    def get_id_map_link(self, link):
        id_corp = ''
        for x in self.dict_id_url:
            if link == x[1]:
                id_corp = x[0]
                break
        for i in range(len(self.list_id_corp)):
            if id_corp == self.list_id_corp[i]:
                return i
        return ''

    def login(self, response):
        """Generate a login request."""
        return FormRequest.from_response(response,
                                         formdata={'name': 'herman', 'password': 'password'},
                                         formxpath='/form[@class="login"]',
                                         callback=self.check_login_response)

    def check_login_response(self, response):
        """Check the response returned by a login request to see if we are
        successfully logged in.
        """
        if "logout" in response.body:
            self.log("Successfully logged in. Let's start crawling!")
            # Now the crawling can begin..
            return self.initialized()
        else:
            self.log("Bad times :(")
            # Something went wrong, we couldn't log in, so nothing happens.