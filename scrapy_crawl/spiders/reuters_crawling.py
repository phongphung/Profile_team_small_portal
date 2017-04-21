__author__ = 'sunary'


import pandas as pd
import urllib
from utils import my_helper
from scrapy.http import Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "reuters_crawling"
    # allowed_domains = []
    start_urls = []
    paths = []
    mongodb = None
    mongodb_save = None
    list_id_corp = []
    list_name_corps = []

    def init_urls(self):
        #self.login_page = 'http://www.domain.com/login'
        file_list_ids = pd.read_csv('/home/sunary/data_report/crawl_and_check.csv')
        self.list_id_corp = file_list_ids['id']
        self.list_name_corps = file_list_ids['name']

        for legalname_id in range(len(file_list_ids['legalname'])):
            if not my_helper.pandas_null(file_list_ids['legalname'][legalname_id]):
                self.list_name_corps[legalname_id] = file_list_ids['legalname'][legalname_id]

        self.start_urls = []
        for name in self.list_name_corps:
            self.start_urls.append('http://www.reuters.com/finance/stocks/lookup?searchType=any&search=' + str(name))

    def init_request(self):
        """This function is called before crawling starts."""
        return Request(url=self.login_page, callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='reuters_data', col='all_corps')

    def __init__(self, *arg):
        self.init_urls()
        #self.init_request()
        self.init_mongodb()

    def parse(self, response):
        try:
            # time.sleep(2)
            id_corp = self.id_of_link(response.url)
            hxs = HtmlXPathSelector(response)
            all_info = hxs.select('//div[@class="moduleBody"]//td//text()').extract()

            while len(all_info) >= 3:
                condition = {'Id': self.list_id_corp[id_corp],
                             'Symbol': all_info[1],
                             'Name': all_info[0]}
                data = {'Id': self.list_id_corp[id_corp],
                        'Name': all_info[0],
                        'Symbol': all_info[1],
                        'Exchange': all_info[2]}
                # if all_info[0].lower() in self.list_name_corps[id_corp].lower() or self.list_name_corps[id_corp].lower() in all_info[0].lower():
                if self.mongodb_save.count(condition) <= 0:
                    self.mongodb_save.insert(data)

                all_info = all_info[3:]
        except:
            pass

    def id_of_link(self, encode_url):
        encode_url = urllib.unquote_plus(encode_url)
        for id_url in range(len(self.start_urls)):
            if encode_url == self.start_urls[id_url]:
                return id_url
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