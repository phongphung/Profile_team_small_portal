__author__ = 'sunary'


from selenium import webdriver
from utils.my_mongo import Mongodb
from utils.my_helper import init_logger
import time


class MuckrackCrawl():

    def __init__(self):
        self.driver = webdriver.Firefox()
        # self.login({ 'login_page': 'https://muckrack.com/account/login',
        #              'username': 'anders228700@gmail.com',
        #              'password': 'muckrack2012'})

        self.beats_site = ['/directory/afghanistan', '/directory/africa', '/directory/arts', '/directory/australia', '/directory/belgium', '/directory/brazil', '/directory/bizfin', '/directory/canada', '/directory/china', '/directory/crime', '/directory/education', '/directory/egypt', '/directory/energy', '/directory/enviro', '/directory/food', '/directory/france', '/directory/germany', '/directory/health', '/directory/india', '/directory/ireland', '/directory/israel', '/directory/italy', '/directory/japan', '/directory/kenya', '/directory/media', '/directory/metroatl', '/directory/metrobalt', '/directory/metrobos', '/directory/metrochi', '/directory/metrodc', '/directory/metrodallas', '/directory/metrodenver', '/directory/metrodetroit','/directory/metrohouston', '/directory/metrolv', '/directory/metrola',
                           '/directory/metromiami', '/directory/metrominn', '/directory/metronola', '/directory/metrony', '/directory/metrophilly', '/directory/metrophoenix', '/directory/metropittsburgh', '/directory/metrosandiego', '/directory/metrosf', '/directory/metroseattle', '/directory/metrosl', '/directory/mexico', '/directory/middleeast', '/directory/military', '/directory/newzealand', '/directory/nigeria', '/directory/oped', '/directory/pakistan', '/directory/politics', '/directory/realestate', '/directory/religion', '/directory/russia', '/directory/sci', '/directory/singapore', '/directory/southafrica', '/directory/spain', '/directory/sports', '/directory/tech', '/directory/transportation', '/directory/travel', '/directory/natlnews', '/directory/usregional', '/directory/uk', '/directory/weather', '/directory/intl']

        self.mongo = Mongodb(host='localhost', db='muckrack', col='journalist')
        self.logger = init_logger(self.__class__.__name__)

    def login(self, agrs):
        self.driver.get(agrs['login_page'])
        time.sleep(3)
        username = self.driver.find_element_by_xpath('//input[@id="id_username"]')
        username.send_keys(agrs['username'])

        password = self.driver.find_element_by_xpath('//input[@id="id_password"]')
        password.send_keys(agrs['password'])

        # remember = self.driver.find_element_by_class_name('uiInputLabelLabel')
        # remember.click()

        form = self.driver.find_element_by_xpath('//input[@class="btn btn-lg btn-block btn-primary"]')
        form.submit()

    def crawl_beat(self):
        for beat in self.beats_site:
            user_sns_urls = []
            self.logger.info('crawl: http://muckrack.com' + beat)
            self.driver.get('http://muckrack.com' + beat)
            time.sleep(3)

            user_sns_driver = self.driver.find_elements_by_xpath('//div[@class="source-person"]//div[@class="avatar"]/a')
            for driver in user_sns_driver:
                user_sns_urls.append(driver.get_attribute('href'))

            try:
                more_result = self.driver.find_element_by_xpath('//div[@class="endless_container"]/a')
                while more_result.size > 0:
                    self.driver.get(more_result.get_attribute('href'))
                    time.sleep(3)
                    user_sns_driver = self.driver.find_elements_by_xpath('//div[@class="source-person"]//div[@class="avatar"]/a')
                    for driver in user_sns_driver:
                        user_sns_urls.append(driver.get_attribute('href'))

                    more_result = self.driver.find_element_by_xpath('//div[@class="endless_container"]/a')
            except:
                pass

            for url in user_sns_urls:
                self.get_user_sns(url)

    def get_user_sns(self, url_user):
        try:
            self.driver.get(url_user)
        except:
            return

        time.sleep(3)
        user_sns = {'muckrack': url_user}

        try:
            user_sns['name'] = self.driver.find_element_by_xpath('//div[@class="profile-header"]//h1[@class="profile-name"]').text[:-9]
        except:
            pass
        try:
            user_sns['workspace'] = self.driver.find_element_by_xpath('//div[@class="profile-header"]//div[@class="person-details-location"]').text
        except:
            pass
        try:
            user_sns['location'] = self.driver.find_element_by_xpath('//div[@class="profile-header"]//div[@class="person-details-title"]').text
        except:
            pass
        try:
            user_sns['about'] = self.driver.find_element_by_xpath('//div[@class="profile-content"]//p').text
        except:
            pass
        try:
            user_sns['email'] = self.driver.find_element_by_xpath('//div[@class="profile-social-icons"]//a[@class="profile-social-link profile-social-email"]').get_attribute('href')
        except:
            pass
        try:
            user_sns['twitter'] = self.driver.find_element_by_xpath('//div[@class="profile-social-icons"]//a[@class="profile-social-link profile-social-link-twitter"]').get_attribute('href')
        except:
            pass
        try:
            user_sns['facebook'] = self.driver.find_element_by_xpath('//div[@class="profile-social-icons"]//a[@class="profile-social-link profile-social-link-facebook"]').get_attribute('href')
        except:
            pass
        try:
            user_sns['linkedin'] = self.driver.find_element_by_xpath('//div[@class="profile-social-icons"]//a[@class="profile-social-link profile-social-link-linkedin"]').get_attribute('href')
        except:
            pass
        try:
            user_sns['klout'] = self.driver.find_element_by_xpath('//div[@class="profile-social-icons"]//a[@class="profile-social-link profile-social-link-klout"]').get_attribute('href')
        except:
            pass
        try:
            user_sns['website'] = self.driver.find_element_by_xpath('//div[@class="profile-social-icons"]//a[@class="profile-social-link"]').get_attribute('href')
        except:
            pass

        self.mongo.insert(user_sns)

    def close(self):
        self.driver.close()


if __name__ == '__main__':
    muckrack = MuckrackCrawl()
    muckrack.crawl_beat()
    muckrack.close()