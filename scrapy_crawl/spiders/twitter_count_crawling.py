__author__ = 'sunary'


import pandas as pd
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector


class AutoRobot(BaseSpider):
    name = "twitter_count_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 1

    def init_urls(self):
        fo = open('count_follower_following.txt', 'w')
        fo.write('')
        fo.close()
        pd_file = pd.read_csv('/home/sunary/data_report/18May2015_172 IR organize_18May2015byCam.csv')
        self.start_urls = []
        for screen_name in pd_file['Name ID']:
            self.start_urls.append('https://twitter.com/' + str(screen_name[1:]))

    def __init__(self, *arg):
        self.init_urls()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        following = hxs.select(
            "//li[@class='ProfileNav-item ProfileNav-item--following']//span[@class='ProfileNav-value']//text()").extract()
        follower = hxs.select(
            "//li[@class='ProfileNav-item ProfileNav-item--followers']//span[@class='ProfileNav-value']//text()").extract()

        if following and follower:
            fo = open('count_follower_following.txt', 'a')
            fo.write(response.url + ': ')
            fo.write(str(following[0]) + ' ' + str(follower[0]) + '\n')
            fo.close()