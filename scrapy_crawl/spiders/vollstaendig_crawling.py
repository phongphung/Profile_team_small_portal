# -*- coding: utf-8 -*-
__author__ = 'sunary'


import re
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb


class AutoRobot(BaseSpider):
    name = "voll_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 10

    def init_urls(self):
        #crawl word
        self.start_urls = ['http://www.verblisten.de/listen/verben/anfangsbuchstabe/vollstaendig.html']
        for id in range(2, 128):
            self.start_urls.append('http://www.verblisten.de/listen/verben/anfangsbuchstabe/vollstaendig-'+ str(id) + '.html')

        #crawl list_word
        self.start_urls = []
        res = self.mongodb_save.find({},['word', 'list_word'])
        count = 0
        for doc in res:
            if not doc.get('list_word') and self.mongodb_save.count({'word': 'mich_' + doc.get('word')}) <= 0 and self.mongodb_save.count({'word': doc.get('word') + '_(hat)'}) <= 0 and self.mongodb_save.count({'word': doc.get('word') + '_(reg)'}) <= 0:
                self.start_urls.append('http://www.verbformen.de/konjugation/' + self.germany_latin(doc.get('word')) + '.htm')
            else:
                count += 1
        print count


    def init_request(self):
        """This function is called before crawling starts."""
        # return Request(url=self.login_page, callback=self.login)
        pass

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='voll', col='word')

    def __init__(self, *arg):
        self.init_mongodb()
        self.init_urls()
        #self.init_request()

    def parse_page(self, response):
        try:
            hxs = HtmlXPathSelector(response)
            word = hxs.select("//span[@class='wort']//text()").extract()
            for w in word:
                if self.mongodb_save.count({'word': w}) <= 0:
                    self.mongodb_save.insert({'word': w})
        except:
            pass

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        tr_tag = hxs.select("//table[@class='v']//tr").extract()
        match = re.search('konjugation/(.+)\.htm', response.url)
        from_word = self.latin_germany(match.group(1).replace('-', ''))

        list_word = []
        for tag in tr_tag:
            match = re.findall(u'<b>.*?\</b>|\(|\)|\s', tag)
            word = ''
            for m in match:
                m = m.replace('<b>','')
                m = m.replace('</b>','')
                word += m
            word = word.replace('()', '')
            word = re.sub('\s+',' ', word)

            try:
                while len(word) > 0 and word[0] == ' ':
                    word = word[1:]
                while len(word) > 0 and word[-1] == ' ':
                    word = word[:-1]
                list_word.append(word)
            except Exception, e:
                pass

        self.mongodb_save.update({'word':from_word}, {'list_word': ','.join(list_word)}, upsert=True)

    list_latin = ['a:', 'o:', 'u:', 's:']
    list_germany = [u'ä', u'ö', u'ü', u'ß']
    def latin_germany(self, word):
        for i in range(4):
            word = word.replace(self.list_latin[i], self.list_germany[i])
        return word

    def germany_latin(self, word):
        for i in range(4):
            word = word.replace(self.list_germany[i], self.list_latin[i])
        return word
