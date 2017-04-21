__author__ = 'sunary'


from selenium import webdriver
from utils.my_mongo import Mongodb
from selenium.webdriver.support.select import Select
import time


class SGXCrawl():

    def __init__(self):
        self.driver = webdriver.Firefox()
        self.mongo = Mongodb(host='localhost', db='sgx', col='name')

    def crawl_name(self):
        self.driver.get('http://www.sgx.com/wps/portal/sgxweb/home/company_disclosure/stock_indiceslist')
        time.sleep(2)

        # select dropdown list
        dropdown = Select(self.driver.find_element_by_xpath('//select[@id="MarketIdns_Z7_2AA4H0C098FQE0I104R8P121I1_"]'))
        dropdown.select_by_visible_text('SGX MAINBOARD')
        time.sleep(3)

        for _ in range(49):
            name_elements = self.driver.find_elements_by_xpath('//table[@class="sgxTableGrid"]//tr//td/a')
            for elem in name_elements:
                self.mongo.insert({'name': elem.text})

            self.driver.find_element_by_xpath('//div[@id="PageIdnextIdns_Z7_2AA4H0C098FQE0I104R8P121I1_"]').click()
            time.sleep(3)

    def close(self):
        self.driver.close()


if __name__ == '__main__':
    sgx_crawler = SGXCrawl()
    sgx_crawler.crawl_name()
    sgx_crawler.close()