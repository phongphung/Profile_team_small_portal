__author__ = 'sunary'


import pandas as pd
import re
from scrapy.contrib.spiders.init import InitSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from utils import my_helper, my_text
from utils.my_mongo import Mongodb


class AutoRobot(InitSpider):
    name = 'investor_relations'
    start_urls = []
    wish_list_link = ['investor']

    list_investor_id = []
    list_investor_website = []

    REGEX_NAME = '.*?([A-Z][\w\.\-\']+ [A-Z][\w\.\-\']+.*)'
    REGEX_EMAIL = '.*?([\d\w_\.]+\@[\d\w_\.]+).*'
    REGEX_TEL = '.*?((\+\d*\s)?(\(\+?\d+\)\s?)?\d+[\d\s\-]*\d{2,}).*'

    def init_links(self):
        self.pd_file = pd.read_csv('/home/sunary/data_report/2015mar24_bloomberg_mapping_40k_all.csv')
        for i in range(len(self.pd_file['name'])):
            if not my_helper.pandas_null(self.pd_file['website'][i]):
                self.start_urls.append(self.pd_file['website'][i])

    def __init__(self):
        self.init_links()
        self.init_mongodb()

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost',
                                    db='investor_relation_data',
                                    col='investor_new')

    def parse(self, response):
        domain = self.get_domain(response.url)
        hxs = HtmlXPathSelector(response)

        links_investor = hxs.select('//a/@href').extract()
        for link in list(set(links_investor)):
            # Get domain
            if self.determine_next(link):
                if re.match('^http', link):
                    yield Request(link, callback=self.parse_contact)
                else:
                    yield Request(domain + link, callback=self.parse_url)

    def parse_url(self, response):
        domain = self.get_domain(response.url)
        hxs = HtmlXPathSelector(response)

        links_investor = hxs.select('//a/@href').extract()
        for link in list(set(links_investor)):
            # Get domain
            if self.determine_next(link):
                if re.match('^http', link):
                    yield Request(link, callback=self.parse_contact)
                else:
                    yield Request(domain + link, callback=self.parse_contact)

    def get_domain(self, link):
        regex = re.match('(https?://.+)/.*', link)
        if regex:
            return regex.group(1)
        else:
            return link

    def parse_contact(self, response):
        hxs = HtmlXPathSelector(response)
        title = ''.join(hxs.select('//title/text()').extract())
        all_text = hxs.select('//div/text() | //p/text() | //p/a/text()').extract()

        # remove untext
        len_list_text = len(all_text)
        index_text = 0
        while index_text < len_list_text:
            regex = re.match('.*[0-9A-Za-z].*', all_text[index_text])
            if not regex or len(all_text[index_text]) <= 7:
                del all_text[index_text]
                index_text -= 1
                len_list_text -= 1
            else:
                all_text[index_text] = re.sub('\s+', ' ', all_text[index_text])
            index_text += 1

        for index_text in range(len(all_text)):
            regex_email = re.match(self.REGEX_EMAIL, all_text[index_text])
            if regex_email:
                data = self.find_info(all_text[:index_text])
                if data['name1']:
                    data['company'] = self.get_company_name(my_text.root_url(response.url))
                    data['email'] = all_text[index_text]
                    data['url'] = response.url
                    data['title']= title
                    if self.mongodb_save.count({'title': data['title'], 'email': data['email']}) <= 0:
                        self.mongodb_save.insert(data)
            elif len(all_text[index_text]) < 18:
                regex_tel = re.match(self.REGEX_TEL, all_text[index_text])
                if regex_tel and len(regex_tel.group(1)) >= 7:
                    data = self.find_info(all_text[:index_text])
                    if data['name1']:
                        data['company'] = self.get_company_name(my_text.root_url(response.url))
                        data['tel'] = all_text[index_text]
                        data['url'] = response.url
                        data['title']= title
                        if self.mongodb_save.count({'title': data['title'], 'tel': data['tel']}) <= 0:
                            self.mongodb_save.insert(data)

    def find_info(self, content):
        data = {'name1': '',
                'name2': '',
                'need_detect': ''}

        get_first_name = True
        for i in range(len(content) - 1, -1, -1):
            if len(content[i]) < 30:
                regex_name = re.match(self.REGEX_NAME, content[i])
                if regex_name:
                    if get_first_name:
                        data['name1'] = content[i]
                        data['need_detect'] = ', '.join(content[i:])
                        get_first_name = False
                    else:
                        data['name2'] = content[i]
                        data['need_detect'] = ', '.join(content[i:])
                        return data
        return data

    def determine_next(self, full_link):
        if any(word in full_link for word in self.wish_list_link):
            return True
        return False

    def get_company_name(self, url):
        for i in range(len(self.pd_file['name'])):
            if not my_helper.pandas_null(self.pd_file['website'][i]) and url in self.pd_file['website'][i]:
                return self.pd_file['name'][i]
        return ''