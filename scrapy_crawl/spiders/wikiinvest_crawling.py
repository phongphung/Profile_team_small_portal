__author__ = 'sunary'


from utils import my_helper
import pandas as pd
from scrapy.http import Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb
import re


class AutoRobot(BaseSpider):
    name = "wikiinvest_crawling"

    start_urls = []
    mongodb_save = None
    url_ticker = ''
    DOWNLOAD_DELAY = 5

    def init_urls(self):
        self.start_urls = []
        pd_file = pd.read_csv('/home/sunary/data_report/2015mar24_bloomberg_mapping_40k_all.csv')
        for i in range(len(pd_file['ticker'])):
            if not my_helper.pandas_null(pd_file['ticker'][i]):
                if ':' in str(pd_file['ticker'][i]):
                    ticker_split = str(pd_file['name'][i]).split(':')
                    self.start_urls.append('http://www.wikinvest.com/wiki/%s'% ticker_split[0])
                    if len(ticker_split) > 1:
                        self.start_urls.append('http://www.wikinvest.com/wiki/%s'% ticker_split[1])
                        self.start_urls.append('http://www.wikinvest.com/wiki/%s'% ticker_split[0] + ticker_split[1])
                else:

                    self.start_urls.append('http://www.wikinvest.com/wiki/%s'% pd_file['ticker'][i])

    def init_request(self):
        """This function is called before crawling starts."""
        return Request(url=self.login_page, callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_company = Mongodb(host='localhost', db='wikiinvest_data', col='company')
        self.mongodb_save = Mongodb(host='localhost', db='wikiinvest_data', col='investor')

    def __init__(self, *arg):
        self.init_urls()
        #self.init_request()
        self.init_mongodb()

    # http://www.wikinvest.com/wiki/Apple
    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        data = {'company':hxs.select('//div[@class="liveQuoteTitle"]//text()').extract(),
                'url': response.url,
                'investor': ''}

        all_investor = hxs.select('//div[@class="contributor-name"]//@href').extract()
        for investor in all_investor:
            data['investor'] = 'http://www.wikinvest.com' + investor
            if self.mongodb_company.count(data) <= 0:
                self.mongodb_company.insert(data)

            yield Request('http://www.wikinvest.com' + investor, callback=self.parse_user)

    # http://www.wikinvest.com/user/Jjiang
    def parse_user(self, response):
        data = {'name': '',
                'job_title': '',
                'company': '',
                'about_me': '',
                'topic': '',
                'feed': '',
                'url': response.url}

        hxs = HtmlXPathSelector(response)
        if 'The wikinvest user profile page' in response.body:
            find_all = re.findall('.+profile\spage\sfor\s(.+?) \(.+', response.body)
            data['name'] = find_all[0]
        else:
            find_all = re.findall('.+name=\"description\"\scontent=\"(.+?)\".+', response.body)
            data['about_me'] = find_all[0]
            match = re.match('.+wikinvest.com\/user\/(.+)', response.url)
            data['name'] = match.group(1)

        data['job_title'] = ''.join(hxs.select('//div[@id="wikinvestRankContainer"]//text()').extract())
        res = self.mongodb_company.find({'url': response.request.headers.get('Referer', None)})
        for doc in res:
            data['company'] += ''.join(doc['company']) + '; '

        data['topic'] = '; '.join(hxs.select('//ul[@class="articleList"]//text()').extract())
        data['feed'] = '; '.join(hxs.select('//div[@id="blogWidget_container"]//text()').extract())
        condition = {'name': data['name'],
                     'url': data['url']}

        if self.mongodb_save.count(condition) <= 0:
            self.mongodb_save.insert(data)
