__author__ = 'sunary'


from utils.my_mongo import Mongodb
from selenium import webdriver
import time
import pandas as pd
import re


class GoogleSearch():
    list_user = []
    def __init__(self):
        self.driver = webdriver.Firefox()

    def first_result(self):
        mongo_saver = Mongodb(db='publisher', col='need_url')
        pd_file = pd.read_csv('/home/nhat/data/news_blog need avatar without URL.csv')

        for i in range(len(pd_file['object_id'][340:])):
            self.driver.get('https://www.google.com/#q=%s' % (pd_file['name'][i]))
            try:
                result = self.driver.find_element_by_xpath('//div[@class="rc"]//h3[@class="r"]')
                url = re.match('.+href="(.+?)".+', result.get_attribute('innerHTML'))
                if url:
                    data = {'object_id': pd_file['object_id'][i],
                            'url': url.group(1),
                            'channel': pd_file['object_id'][i],
                            'created_at': pd_file['created_at'][i],
                            'name': pd_file['name'][i]}
                    mongo_saver.insert(data)
            except:
                pass

            time.sleep(10)

    def close(self):
        self.driver.close()

if __name__ == '__main__':
    google_search = GoogleSearch()
    google_search.first_result()
    # my_selenium.close()