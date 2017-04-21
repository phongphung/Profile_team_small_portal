# -*- coding: utf-8 -*-
__author__ = 'sunary'


from scrapy.http import Request, FormRequest
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb
import re
import ast


class AutoRobot(BaseSpider):
    name = "hksfc_crawling"
    DOWNLOAD_DELAY = 5

    def init_urls(self):
        self.start_urls = []
        self.lastfix_page = ['addresses',
                   'ro',
                   'rep',
                   'co',
                   'prev_name',
                   ]
        self.parse_page = [self.parse_addresses,
                 self.parse_ro,
                 self.parse_rep,
                 self.parse_co,
                 self.parse_prev_name,
                 ]

        res = self.mongodb_save.find()
        for doc in res:
            self.start_urls.append(doc['url'])

    def init_request(self):
        """This function is called before crawling starts."""
        return Request(url=self.login_page, callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='hksfc', col='url')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)

        data = {'name': '',
                'regular_activity': ''}

        match = re.findall('.+value\:\sExt\.utils\.Format\.format\((.+?)\),', response.body)
        result_re = match[0].split(', ')
        for rs in result_re[1:]:
            if len(rs) > 2:
                data['name'] += rs[1:-1] + ' '

        match = re.findall('.+raDetailData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['regular_activity'] += rs['actDesc'] + ', '

        self.mongodb_save.update({'url': response.url}, data, upsert=False)

        for i in range(len(self.lastfix_page)):
            yield Request(self.url_edit(response.url, self.lastfix_page[i]), callback=self.parse_page[i])

    def parse_addresses(self, response):
        hxs = HtmlXPathSelector(response)

        data = {'bussiness_add': '',
                'email': '',
                'website': ''}

        match = re.findall('.+addressData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['bussiness_add'] += rs['fullAddress'] + ', '

        match = re.findall('.+emailData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['email'] += rs['email'] + ', '

        match = re.findall('.+websiteData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0 and match[0] != '[null]':
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['website'] += rs['website'] + ', '

        self.mongodb_save.update({'url': self.url_edit(response.url, 'details')}, data, upsert=False)

    def parse_ro(self, response):
        hxs = HtmlXPathSelector(response)

        data = {'ro': ''}

        match = re.findall('.+roData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['ro'] += rs['fullName'] + ', '

        self.mongodb_save.update({'url': self.url_edit(response.url, 'details')}, data, upsert=False)

    def parse_rep(self, response):
        hxs = HtmlXPathSelector(response)

        data = {'rep': ''}

        match = re.findall('.+repData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['rep'] += rs['fullName'] + ', '

        self.mongodb_save.update({'url': self.url_edit(response.url, 'details')}, data, upsert=False)

    def parse_co(self, response):
        hxs = HtmlXPathSelector(response)

        data = {'contact_detail_tel': '',
                'contact_detail_fax': '',
                'contact_detail_email': '',
                'contact_detail_add': '',
                }

        match = re.findall('.+cofficerData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['contact_detail_tel'] += rs['tel'] + ', '
                data['contact_detail_fax'] += rs['fax'] + ', '
                data['contact_detail_email'] += rs['email'] + ', '
                data['contact_detail_add'] += rs['address']['fullAddress'] + ', '

        self.mongodb_save.update({'url': self.url_edit(response.url, 'details')}, data, upsert=False)

    def parse_prev_name(self, response):
        hxs = HtmlXPathSelector(response)

        data = {'prev_eng_name': '',
                'prev_china_name': ''}

        match = re.findall('.+prevNameData\s\=\s(\[.+?\]);', response.body)
        if len(match) > 0:
            result_re = ast.literal_eval(match[0].replace('null', '""'))
            for rs in result_re:
                data['prev_eng_name'] += rs['englishName'] + '(' + rs['changeDate'] + ')' + ', '
                data['prev_china_name'] += rs['chineseName'] + ', '

        self.mongodb_save.update({'url': self.url_edit(response.url, 'details')}, data, upsert=False)

    def url_edit(self, url, lastfix):
        match = re.match('(.+\/)[a-z]+', url)
        return match.group(1) + lastfix
