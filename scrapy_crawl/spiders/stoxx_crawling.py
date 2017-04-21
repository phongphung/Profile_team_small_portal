__author__ = 'sunary'


from scrapy import Selector
from scrapy.http import Request
from scrapy.spider import BaseSpider
from utils.my_mongo import Mongodb
import re


class AutoRobot(BaseSpider):
    name = "stoxx_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    download_delay = 3

    def init_urls(self):
        self.start_urls = ['https://www.stoxx.com/web/stoxxcom/component-details?key=477518', 'https://www.stoxx.com/web/stoxxcom/component-details?key=408530', 'https://www.stoxx.com/web/stoxxcom/component-details?key=490541', 'https://www.stoxx.com/web/stoxxcom/component-details?key=407228', 'https://www.stoxx.com/web/stoxxcom/component-details?key=475531', 'https://www.stoxx.com/web/stoxxcom/component-details?key=425240', 'https://www.stoxx.com/web/stoxxcom/component-details?key=480710',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=401632', 'https://www.stoxx.com/web/stoxxcom/component-details?key=408348', 'https://www.stoxx.com/web/stoxxcom/component-details?key=413366', 'https://www.stoxx.com/web/stoxxcom/component-details?key=476361', 'https://www.stoxx.com/web/stoxxcom/component-details?key=491207', 'https://www.stoxx.com/web/stoxxcom/component-details?key=488082', 'https://www.stoxx.com/web/stoxxcom/component-details?key=448816',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=550190', 'https://www.stoxx.com/web/stoxxcom/component-details?key=511938', 'https://www.stoxx.com/web/stoxxcom/component-details?key=458887', 'https://www.stoxx.com/web/stoxxcom/component-details?key=407683', 'https://www.stoxx.com/web/stoxxcom/component-details?key=454005', 'https://www.stoxx.com/web/stoxxcom/component-details?key=443639', 'https://www.stoxx.com/web/stoxxcom/component-details?key=401140',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=453478', 'https://www.stoxx.com/web/stoxxcom/component-details?key=481775', 'https://www.stoxx.com/web/stoxxcom/component-details?key=426724', 'https://www.stoxx.com/web/stoxxcom/component-details?key=442464', 'https://www.stoxx.com/web/stoxxcom/component-details?key=401225', 'https://www.stoxx.com/web/stoxxcom/component-details?key=483410', 'https://www.stoxx.com/web/stoxxcom/component-details?key=407023',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=423244', 'https://www.stoxx.com/web/stoxxcom/component-details?key=546078', 'https://www.stoxx.com/web/stoxxcom/component-details?key=711131', 'https://www.stoxx.com/web/stoxxcom/component-details?key=481808', 'https://www.stoxx.com/web/stoxxcom/component-details?key=517617', 'https://www.stoxx.com/web/stoxxcom/component-details?key=579802', 'https://www.stoxx.com/web/stoxxcom/component-details?key=430230',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=408522', 'https://www.stoxx.com/web/stoxxcom/component-details?key=EG7', 'https://www.stoxx.com/web/stoxxcom/component-details?key=B0C2CQ', 'https://www.stoxx.com/web/stoxxcom/component-details?key=461075', 'https://www.stoxx.com/web/stoxxcom/component-details?key=461785', 'https://www.stoxx.com/web/stoxxcom/component-details?key=430376', 'https://www.stoxx.com/web/stoxxcom/component-details?key=491134',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=405671', 'https://www.stoxx.com/web/stoxxcom/component-details?key=468520', 'https://www.stoxx.com/web/stoxxcom/component-details?key=463841', 'https://www.stoxx.com/web/stoxxcom/component-details?key=476837',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=492752', 'https://www.stoxx.com/web/stoxxcom/component-details?key=417754', 'https://www.stoxx.com/web/stoxxcom/component-details?key=473322', 'https://www.stoxx.com/web/stoxxcom/component-details?key=473550']
        self.start_urls = self.start_urls[:1]

    def init_mongodb(self):
        self.mongodb_save = Mongodb(host='localhost', db='stoxx', col='ticker')

    def __init__(self):
        self.init_urls()
        self.init_mongodb()

    def parse(self, response):
        regex_name = re.match('.+<title>(.+?)</title>.+', response.body)
        regex_name = regex_name.group(1)[16:] if regex_name else ''
        regex_isin = re.match('.+<td>ISIN\:.*?</td>.*?<td>(.+?)</td>.+', response.body)
        regex_isin = regex_isin.group(1) if regex_isin else ''
        regex_ric = re.match('.+<td>RIC\:.*?</td>.*?<td>(.+?)</td>.+', response.body)
        regex_ric = regex_ric.group(1) if regex_ric else ''

        data = {'name': regex_name,
                'isin': regex_isin,
                'ric': regex_ric}
        print data
        # self.mongodb_save.insert(data)
