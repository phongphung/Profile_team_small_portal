__author__ = 'sunary'


import re
from selenium.webdriver.support.select import Select
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from utils.my_mongo import Mongodb
import time


class HKsfc():
    def __init__(self):
        self.driver = webdriver.Firefox()
        self.url = 'http://www.sfc.hk/publicregWeb/searchByRa'

        self.mongo = Mongodb(db = 'hksfc',col= 'url')

    def crawl(self):
        self.driver.get(self.url)
        time.sleep(1)

        list_types = ['radiofield-1019-inputEl',
                         'radiofield-1020-inputEl',
                         'radiofield-1021-inputEl',
                         'radiofield-1022-inputEl',
                         'radiofield-1023-inputEl',
                         'radiofield-1024-inputEl',
                         'radiofield-1025-inputEl',
                         'radiofield-1026-inputEl',
                         'radiofield-1027-inputEl',
                         'radiofield-1028-inputEl',
                         ]

        list_character_start = [chr(i + ord('A')) for i in range(26)]
        list_character_start += [chr(i + ord('0')) for i in range(10)]

        #choose corporation
        self.driver.find_element_by_xpath('//input[@id="roleTypeCorporation-inputEl"]').click()

        #choose type
        for i in range(len(list_types)):
            self.driver.find_element_by_xpath('//input[@id="%s"]' % list_types[i]).click()
            #choose character start
            for j in range(len(list_character_start)):
                elem = self.driver.find_element_by_xpath('//input[@id="combobox-1010-inputEl"]')
                self.driver.execute_script('''
                    var elem = arguments[0];
                    var value = arguments[1];
                    elem.value = value;
                ''', elem, list_character_start[j])

                #press search
                self.driver.find_element_by_xpath('//button[@id="button-1011-btnEl"]').click()
                self.get_url()

                while True:
                    check_have_next = self.driver.find_element_by_xpath('//em[@id="button-1055-btnWrap"]').get_attribute('innerHTML')
                    if 'disabled' in check_have_next:
                        break
                    self.driver.find_element_by_xpath('//button[@id="button-1055-btnEl"]').click()
                    self.get_url()

    def get_url(self):
        time.sleep(4)
        table_info = self.driver.find_element_by_xpath('//table[@class="x-grid-table x-grid-table-resizer"]')
        inner_html = table_info.get_attribute('innerHTML')
        find_all = re.findall('href="(.+?)"', inner_html)
        for url in find_all:
            if self.mongo.count({'url': 'http://www.sfc.hk' + url}) <= 0:
                self.mongo.insert({'url': 'http://www.sfc.hk' + url})

    def close(self):
        self.driver.close()

if __name__ == '__main__':
    sfc = HKsfc()
    sfc.crawl()
    sfc.close()