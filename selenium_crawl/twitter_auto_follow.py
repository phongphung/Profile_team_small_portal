__author__ = 'sunary'


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from random import randint
import time
import pandas as pd


class AutoFollow():

    def __init__(self):
        self.driver = webdriver.Firefox()
        self.login({ 'login_page': 'https://twitter.com/login',
                     'username': 'v2nhat@gmail.com',
                     'password': '******'})

    def login(self, agrs):
        self.driver.get(agrs['login_page'])
        time.sleep(5)
        username = self.driver.find_element_by_xpath('//div[@id="page-outer"]//*[@class="js-username-field email-input js-initial-focus"]')
        username.send_keys(agrs['username'])

        password = self.driver.find_element_by_xpath('//div[@id="page-outer"]//*[@class="js-password-field"]')
        password.send_keys(agrs['password'])

        # remember = self.driver.find_element_by_class_name('uiInputLabelLabel')
        # remember.click()

        form = self.driver.find_element_by_xpath('//div[@id="page-outer"]//*[@class="submit btn primary-btn"]')
        form.submit()

    def get_users(self):
        pd_file = pd.read_csv('')
        self.list_user = pd_file['screenname']

    def auto_click(self):
        for user in self.list_user:
            self.driver.get("https://twitter.com/" + user)
            time.sleep(randint(7,20))
            button_follow = self.driver.find_element_by_xpath('//div[@id="page-outer"]//*[@class="user-actions-follow-button js-follow-btn follow-button btn"]')
            if 'Follow' in button_follow.text:
                button_follow.click()

    def close(self):
        self.driver.close()

if __name__ == '__main__':
    auto_follow = AutoFollow()
    auto_follow.auto_click()
    auto_follow.close()