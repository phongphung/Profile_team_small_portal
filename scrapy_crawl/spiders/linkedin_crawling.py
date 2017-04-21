__author__ = 'sunary'


from scrapy.http import Request, FormRequest
from scrapy.contrib.spiders.init import InitSpider
from scrapy.selector import HtmlXPathSelector
import time
from random import uniform
from utils.my_mongo import Mongodb


class AutoRobot(InitSpider):
    name = "linkedin_crawling"
    allowed_domains = ['linkedin.com']
    login_page = 'https://www.linkedin.com/uas/login'
    start_urls = []

    download_delay = 3

    def init_urls(self):
        mongo_url = Mongodb(host='localhost',
                            db='linkedin_data',
                            col='url_search')
        res = mongo_url.find({'keyword': self.keyword[self.id_keyword]}, ['url'])
        for doc in res:
            self.start_urls.append(doc['url'])

    def init_request(self):
        return Request(url=self.login_page,
                       callback=self.login)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost',
                                    db='linkedin_data',
                                    col='by_keyword')

    def __init__(self, *arg):
        self.keyword = ['Investor relations Professional', 'Investor Relations Specialist', 'IR Professional', 'IR Specialist', 'Investor relations Officer', 'Investor relations', 'Investor relations Departtment',
                   'Investor Relations Expert', 'IR Director', 'IR services', 'IR consultant', 'investor relations consultancy', 'investor relations pro', 'IR Analyst', 'IR Executive', 'Investor relation']
        self.id_keyword = 0
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

            personProfile['keyword'] = self.keyword[self.id_keyword]
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
            self.mongodb_save.insert(personProfile)

        except:
            print 'have problem!'

    def login(self, response):
        return FormRequest.from_response(response,
                                         # formname='login',
                                         formdata={'session_key': self.settings['LINKEDIN_ACCOUNT4'],
                                                   'session_password': self.settings['LINKEDIN_PASSWORD4']},
                                         # formxpath='/*form[@id="login"]',
                                         callback=self.check_login_response)

    def check_login_response(self, response):
        if "Sign Out" in response.body:
            print "Successfully logged in. Let's start crawling!"
            return self.initialized()
        else:
            print "Bad times :("