__author__ = 'sunary'


from selenium import webdriver
from utils.my_mongo import Mongodb
import time


class StoxxCrawl():

    def __init__(self):
        self.driver = webdriver.Firefox()
        self.mongo = Mongodb(host='localhost', db='stoxx', col='ticker')

    def crawl_name(self):
        list_urls = ['https://www.stoxx.com/web/stoxxcom/component-details?key=477518', 'https://www.stoxx.com/web/stoxxcom/component-details?key=408530', 'https://www.stoxx.com/web/stoxxcom/component-details?key=490541', 'https://www.stoxx.com/web/stoxxcom/component-details?key=407228', 'https://www.stoxx.com/web/stoxxcom/component-details?key=475531', 'https://www.stoxx.com/web/stoxxcom/component-details?key=425240', 'https://www.stoxx.com/web/stoxxcom/component-details?key=480710',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=401632', 'https://www.stoxx.com/web/stoxxcom/component-details?key=408348', 'https://www.stoxx.com/web/stoxxcom/component-details?key=413366', 'https://www.stoxx.com/web/stoxxcom/component-details?key=476361', 'https://www.stoxx.com/web/stoxxcom/component-details?key=491207', 'https://www.stoxx.com/web/stoxxcom/component-details?key=488082', 'https://www.stoxx.com/web/stoxxcom/component-details?key=448816',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=550190', 'https://www.stoxx.com/web/stoxxcom/component-details?key=511938', 'https://www.stoxx.com/web/stoxxcom/component-details?key=458887', 'https://www.stoxx.com/web/stoxxcom/component-details?key=407683', 'https://www.stoxx.com/web/stoxxcom/component-details?key=454005', 'https://www.stoxx.com/web/stoxxcom/component-details?key=443639', 'https://www.stoxx.com/web/stoxxcom/component-details?key=401140',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=453478', 'https://www.stoxx.com/web/stoxxcom/component-details?key=481775', 'https://www.stoxx.com/web/stoxxcom/component-details?key=426724', 'https://www.stoxx.com/web/stoxxcom/component-details?key=442464', 'https://www.stoxx.com/web/stoxxcom/component-details?key=401225', 'https://www.stoxx.com/web/stoxxcom/component-details?key=483410', 'https://www.stoxx.com/web/stoxxcom/component-details?key=407023',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=423244', 'https://www.stoxx.com/web/stoxxcom/component-details?key=546078', 'https://www.stoxx.com/web/stoxxcom/component-details?key=711131', 'https://www.stoxx.com/web/stoxxcom/component-details?key=481808', 'https://www.stoxx.com/web/stoxxcom/component-details?key=517617', 'https://www.stoxx.com/web/stoxxcom/component-details?key=579802', 'https://www.stoxx.com/web/stoxxcom/component-details?key=430230',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=408522', 'https://www.stoxx.com/web/stoxxcom/component-details?key=EG7', 'https://www.stoxx.com/web/stoxxcom/component-details?key=B0C2CQ', 'https://www.stoxx.com/web/stoxxcom/component-details?key=461075', 'https://www.stoxx.com/web/stoxxcom/component-details?key=461785', 'https://www.stoxx.com/web/stoxxcom/component-details?key=430376', 'https://www.stoxx.com/web/stoxxcom/component-details?key=491134',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=405671', 'https://www.stoxx.com/web/stoxxcom/component-details?key=468520', 'https://www.stoxx.com/web/stoxxcom/component-details?key=463841', 'https://www.stoxx.com/web/stoxxcom/component-details?key=476837',
                            'https://www.stoxx.com/web/stoxxcom/component-details?key=492752', 'https://www.stoxx.com/web/stoxxcom/component-details?key=417754', 'https://www.stoxx.com/web/stoxxcom/component-details?key=473322', 'https://www.stoxx.com/web/stoxxcom/component-details?key=473550']

        for url in list_urls:
            self.driver.get(url)
            time.sleep(2)

            data = {}
            try:
                data['name'] = self.driver.find_element_by_xpath('//div[@class="large-8 small-12 columns nopadding contact-right-topText"]/h1').text
            except:
                pass
            try:
                table_element = self.driver.find_elements_by_xpath('//div[@class="left large-6 small-12 columns"]//tr/td')
                data['isin'] = table_element[3].text
                data['iric'] = table_element[9].text
            except:
                pass

            self.mongo.insert(data)

    def close(self):
        self.driver.close()

if __name__ == '__main__':
    stoxx = StoxxCrawl()
    stoxx.crawl_name()
    stoxx.close()