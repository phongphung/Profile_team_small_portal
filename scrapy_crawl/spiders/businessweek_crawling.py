__author__ = 'sunary'

import urllib
import pandas as pd
from utils import my_helper
from scrapy.http import Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector

from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "businessweek_crawling"
    # allowed_domains = []
    start_urls = []
    paths = []
    mongodb = None
    mongodb_save = None
    list_id_corp = []
    list_name_corps = []
    list_website_corps = []
    dict_id_ticker = []

    url_ticker = ''

    def init_urls(self):
        # self.login_page = 'http://www.domain.com/login'
        self.url_ticker = 'http://www.bloomberg.com/research/stocks/snapshot/snapshot.asp?ticker='
        file_list_ids = pd.read_csv('/home/sunary/data_report/crawl_and_check.csv')
        self.list_id_corp = file_list_ids['id']
        self.list_name_corps = file_list_ids['name']

        for legalname_id in range(len(file_list_ids['legalname'])):
            if not my_helper.pandas_null(file_list_ids['legalname'][legalname_id]):
                self.list_name_corps[legalname_id] = file_list_ids['legalname'][legalname_id]

        # self.list_website_corps = file_list_ids['www']
        self.start_urls = []
        for name in self.list_name_corps:
            self.start_urls.append(
                'http://investing.businessweek.com/research/common/symbollookup/symbollookup.asp?region=ALL&letterIn=' + str(
                    name))
            # self.start_urls = ['http://investing.businessweek.com/research/common/symbollookup/symbollookup.asp?region=ALL&letterIn=Smith (DS)']
            #scrapy crawl google_finance_crawling -o items.csv -t csv
            #mongoexport --db reuters_data --collection corps --csv --fields Name,Symbol,Exchange --out corps.csv

    def init_request(self):
        """This function is called before crawling starts."""
        return Request(url=self.login_page, callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='businessweek_data', col='all_corps')

    def __init__(self, *arg):
        self.init_urls()
        #self.init_request()
        self.init_mongodb()

    def parse(self, response):
        # time.sleep(2)
        id_corp = self.id_of_link(response.url)
        hxs = HtmlXPathSelector(response)

        all_info = hxs.select('//table[@class="table"]/tr/td//text()').extract()
        id_ticker = 0
        while len(all_info) >= 4 and id_ticker < len(all_info):
            self.dict_id_ticker.append([self.list_id_corp[id_corp], all_info[id_ticker]])
            yield Request(self.url_ticker + all_info[id_ticker], callback=self.parse_detail)
            # print url_ticker + all_info[id_ticker]
            id_ticker += 4

    def parse_detail(self, response):
        try:
            id_corp = self.id_of_link(response.url)
            hxs = HtmlXPathSelector(response)
            all_info = hxs.select('//h2[@class="pageHeader"]//text()').extract()
            website_field = hxs.select(
                '//div[@class="detailsDataContainerLt"]//a[@class="link_sb_blue bold"]//text()').extract()

            Name = ''
            Symbol = ''
            Exchange = ''
            Website = ''
            if len(all_info) > 3:
                Name = all_info[0]
                Exchange = all_info[-2]

            Website = website_field[0] if len(website_field) > 0 else ''
            Symbol = response.url.split('=')[1]

            id_corp = self.get_id_map_ticker(Symbol)

            condition = {'Id': self.list_id_corp[id_corp],
                         'Symbol': Symbol,
                         'Name': Name}
            data = {'Id': self.list_id_corp[id_corp],
                    'Name': Name,
                    'Symbol': Symbol,
                    'Exchange': Exchange,
                    'Website': Website}
            # if len(self.list_website_corps[id_corp]) > 4 and len(Website) > 4 and (Website.lower() in self.list_website_corps[id_corp].lower() or self.list_website_corps[id_corp].lower() in Website.lower()):
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

    def get_id_map_ticker(self, ticker):
        id_corp = ''
        for x in self.dict_id_ticker:
            if ticker == x[1]:
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