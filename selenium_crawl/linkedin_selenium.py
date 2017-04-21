__author__ = 'sunary'


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import random
from utils.my_mongo import Mongodb


class LinkedinGetUrl():
    def __init__(self):
        self.driver = webdriver.Firefox()
        self.login({ 'login_page': 'https://www.linkedin.com/uas/login',
                     'username': 'v2nhat@gmail.com',
                     'password': 'nhat123456' })

        self.mongo_save = Mongodb(db='linkedin_data', col='url_search')


    def login(self, agrs):
        self.driver.get(agrs['login_page'])

        username = self.driver.find_element_by_id('session_key-login')
        username.send_keys(agrs['username'])

        password = self.driver.find_element_by_id('session_password-login')
        password.send_keys(agrs['password'])

        button_login = self.driver.find_element_by_id('btn-primary')
        button_login.submit()
        time.sleep(5)

    def crawl(self):
        keyword = ['Investor relations Professional', 'Investor Relations Specialist', 'IR Professional', 'IR Specialist', 'Investor relations Officer', 'Investor relations', 'Investor relations Departtment',
                   'Investor Relations Expert', 'IR Director', 'IR services', 'IR consultant', 'investor relations consultancy', 'investor relations pro', 'IR Analyst', 'IR Executive', 'Investor relation']
        for kw in keyword:
            self.driver.get('https://www.linkedin.com/')
            time.sleep(random.uniform(5, 10))

            search_keyword = self.driver.find_element_by_id('main-search-box')
            search_keyword.send_keys('')
            search_keyword.send_keys(kw)

            search_button = self.driver.find_element_by_class_name('search-button')
            search_button.submit()

            while True:
                time.sleep(random.uniform(5, 10))
                for i in range(1, 11):
                    string_query = '//*[@class="search-results"]/li[' + str(i) + ']/a'
                    string_query = '//li[@class="mod result idx' + str(i) + ' people"]/a'
                    try:
                        linkedin_id = self.driver.find_element_by_xpath(string_query)

                        data = {'keyword': kw,
                                'url': linkedin_id.get_attribute('href')}
                        self.mongo_save.insert(data)
                    except:
                        pass

                check_have_next = self.driver.find_element_by_xpath('//div[@id="results-col"]').get_attribute('innerHTML')
                if 'Next Page' not in check_have_next:
                    break

                button_next = self.driver.find_element_by_xpath('//li[@class="next"]')
                button_next.click()

    def close(self):
        self.driver.close()

if __name__ == '__main__':
    linkedin = LinkedinGetUrl()
    linkedin.crawl()
    linkedin.close()

