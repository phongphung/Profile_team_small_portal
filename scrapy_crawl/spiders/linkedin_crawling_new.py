__author__ = 'sunary'


from scrapy.http import Request, FormRequest
from scrapy.contrib.spiders.init import InitSpider
from scrapy.selector import HtmlXPathSelector
import time
import pandas as pd
import urllib
import re
from random import uniform
from utils.my_mongo import Mongodb


class AutoRobot(InitSpider):
    name = "linkedin_crawling_new"
    allowed_domains = ['linkedin.com']
    login_page = 'https://www.linkedin.com/uas/login'
    start_urls = []

    paths = []
    mongodb = None
    mongodb_save = None
    download_delay = 3

    list_id = []
    list_urls = []

    def init_urls(self):
        file_list_ids = pd.read_csv('/home/sunary/data_report/list_url_linkedin_20150415.csv')
        for i in range(239000, len(file_list_ids['_id'])):
            self.list_urls.append(self.format_url(file_list_ids['url'][i]))
            self.list_id.append(file_list_ids['_id'][i])

        self.start_urls = self.list_urls[0:1000]

    def format_url(self, url):
        link = re.search('.*(linkedin.*)', url)
        return 'https://www.' + link.group(1) if link else 'https://www.linkedin.com'


    def init_request(self):
        """This function is called before crawling starts."""
        return Request(url=self.login_page,
                       callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost',
                                    db='linkedin_user_data',
                                    col='new')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()

    def parse(self, response):
        try:
            time.sleep(uniform(1, 8))
            personProfile = {}
            hxs = HtmlXPathSelector(response)

            name = hxs.select("//span[@class='full-name']//text()").extract()
            title = hxs.select('//div[@id="headline"]//text()').extract()
            location = hxs.select('//span[@class="locality"]/a//text()').extract()
            industry = hxs.select('//dd[@class="industry"]/a//text()').extract()

            overview_summary_current = hxs.select('//tr[@id="overview-summary-current"]/td//text()').extract()
            overview_summary_past = hxs.select('//tr[@id="overview-summary-past"]/td//text()').extract()
            overview_sumary_education = hxs.select('//tr[@id="overview-summary-education"]/td//text()').extract()

            linkedin_link = [response.url]
            linkedin_link.append(hxs.xpath('//div[@id="contact-public-url-view"]//text()').extract())
            linkedin_link.append(hxs.select('//dd[@class="view-public-profile"]/a/@href').extract())
            linkedin_link.append(hxs.select('//a[@class="view-public-profile"]/@href').extract())

            big_summary = hxs.select('//div[@class="summary"]//text()').extract()
            number_connection = hxs.select('//div[@class="member-connections"]/strong//text()').extract()

            email = hxs.select('//div[@id="relationship-emails-view"]/li/a//text()').extract()
            birthday = hxs.select('//div[@id="relationship-birthday-view"]//text()').extract()
            phone = hxs.select('//div[@id="relationship-phone-numbers-view"]/li//text()').extract()

            website = hxs.select('//div[@id="relationship-sites-view"]/li/a/@href').extract()

            span_skill = hxs.select('//span[@class="endorse-item-name"]//text()').extract()
            span_past_experience = hxs.select(
                '//div[@class="editable-item section-item past-position"]/div/header//text()').extract()

            image = hxs.select('//div[@class="profile-picture"]/a/img/@src').extract()

            twitter = hxs.select('//div[@id="twitter-view"]/li/a/@href').extract()

            id_database_user = self.id_of_link(response.url)
            # print id_database_user
            personProfile['_id'] = self.list_id[id_database_user]

            personProfile['twitter'] = ';'.join(twitter)
            personProfile['name_linkedin'] = ' '.join(name)
            personProfile['title'] = ' '.join(title)
            personProfile['location'] = ';'.join(location)
            personProfile['industry'] = ';'.join(industry)
            personProfile['current_company'] = ';'.join(overview_summary_current)
            personProfile['past_company'] = ';'.join(overview_summary_past)
            personProfile['education'] = ';'.join(overview_sumary_education)
            personProfile['url'] = ';'.join(filter(lambda x: x, linkedin_link))

            personProfile['summary'] = ' '.join(big_summary)
            personProfile['connection'] = number_connection[0] if len(number_connection) > 0 else ''
            personProfile['email'] = ';'.join(email)
            personProfile['website'] = ';'.join(website)
            personProfile['birthday'] = ';'.join(birthday)
            personProfile['phone'] = ';'.join(phone)

            personProfile['skill'] = ';'.join(span_skill)
            personProfile['experience'] = ';'.join(span_past_experience)
            personProfile['image'] = image[0] if len(image) > 0 else ''
            if name and self.mongodb_save.count({'_id': personProfile['_id']}) <= 0:
                self.mongodb_save.insert(personProfile)

        except:
            print 'can not match id'

    def id_of_link(self, encode_url):
        encode_url = urllib.unquote_plus(encode_url)
        for id_url in range(len(self.list_urls)):
            if encode_url == self.list_urls[id_url]:
                return id_url
        linkedin_id = re.match('.+profile\/(.+)\/name.+', encode_url)
        if linkedin_id:
            for id_url in range(len(self.list_urls)):
                if linkedin_id.group(1) in self.list_urls[id_url]:
                    return id_url
        linkedin_id = re.match('.+view\?id\=(.+)\&authType.+', encode_url)
        if linkedin_id:
            for id_url in range(len(self.list_urls)):
                if linkedin_id.group(1) in self.list_urls[id_url]:
                    return id_url

        return ''

    def login(self, response):
        """Generate a login request."""
        return FormRequest.from_response(response,
                                         # formname='login',
                                         formdata={'session_key': self.settings['LINKEDIN_ACCOUNT4'],
                                                   'session_password': self.settings['LINKEDIN_PASSWORD4']},
                                         # formxpath='/*form[@id="login"]',
                                         callback=self.check_login_response)

    def check_login_response(self, response):
        if "Sign Out" in response.body:
            print "Successfully logged in. Let's start crawling!"
            # Now the crawling can begin..
            return self.initialized()
        else:
            print "Bad times :("
            # Something went wrong, we couldn't log in, so nothing happens.
