__author__ = 'sunary'


from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from utils.my_mongo import Mongodb
import re


class AutoRobot(BaseSpider):
    name = "universities_crawling"
    # allowed_domains = []
    start_urls = []
    mongodb_save = None
    DOWNLOAD_DELAY = 10

    def init_urls(self):
        list_iso = ['af', 'al', 'dz', 'ad', 'ao', 'ag', 'ar', 'am', 'au', 'at', 'az', 'bs', 'bh', 'bd', 'bb', 'by', 'be', 'bz', 'bj', 'bm', 'bt', 'bo', 'ba', 'bw', 'br', 'bn', 'bg', 'bf', 'mm', 'bi', 'kh', 'cm', 'ca', 'cv', 'ky', 'cf', 'td', 'cl', 'cn', 'co', 'cd', 'cg', 'cr', 'ci', 'hr', 'cu', 'cy', 'cz', 'dk', 'dj', 'dm', 'do', 'ec', 'eg', 'sv', 'gq', 'er', 'ee', 'et', 'fo', 'fj', 'fi', 'fr', 'gf', 'pf', 'ga', 'gm', 'ge', 'de', 'gh', 'gr', 'gl', 'gd', 'gp', 'gu', 'gt', 'gn', 'gy', 'ht', 'va', 'hn', 'hk', 'hu', 'is', 'in', 'id', 'ir', 'iq', 'ie', 'il', 'it', 'jm', 'jp', 'jo', 'kz', 'ke', 'kp', 'kr', 'kv', 'kw', 'kg', 'la', 'lv', 'lb', 'ls', 'lr', 'ly', 'li', 'lt', 'lu', 'mo', 'mk', 'mg', 'mw', 'my', 'mv', 'ml', 'mt', 'mq', 'mr', 'mu', 'mx', 'md', 'mc', 'mn', 'me', 'ms', 'ma', 'mz', 'na', 'np', 'nl', 'an', 'nc', 'nz', 'ni', 'ne', 'ng', 'nu', 'no', 'om', 'pk', 'ps', 'pa', 'pg', 'py', 'pe', 'ph', 'pl', 'pt', 'pr', 'qa', 're', 'ro', 'ru', 'rw', 'kn', 'lc', 'vc', 'ws', 'sm', 'sa', 'sn', 'rs', 'sc', 'sl', 'sg', 'sk', 'si', 'sb', 'so', 'za', 'ss', 'es', 'lk', 'sd', 'sr', 'sz', 'se', 'ch', 'sy', 'tw', 'tj', 'tz', 'th', 'tg', 'to', 'tt', 'tn', 'tr', 'tm', 'tc', 'ug', 'ua', 'ae', 'uk', 'uy', 'uz', 've', 'vn', 'vi', 'ye', 'zm', 'zw']
        self.start_urls = []
        for iso in list_iso:
            self.start_urls.append('http://univ.cc/search.php?dom=' + iso)

    def init_mongodb(self, *args):
        self.mongodb_save = Mongodb(host='localhost', db='university', col='info')

    def __init__(self, *arg):
        self.init_urls()
        self.init_mongodb()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        town = hxs.select("//select[@name='town']").extract()
        if town:
            id_town = re.findall('value="(\d+)"', town[0])
            for id in id_town:
                yield Request('http://univ.cc/search.php?town=' + id, callback=self.parse_town)

    def parse_town(self, response):
        hxs = HtmlXPathSelector(response)
        town = hxs.select("//header/h3/text()").extract()
        country = hxs.select("//header/h3/a/text()").extract()
        university_name = hxs.select("//table[@class='fixedWidth']//a/text()").extract()
        university_url = hxs.select("//table[@class='fixedWidth']//a/@href").extract()

        for id in range(len(university_name)):
            data = {'name': university_name[id],
                    'url': university_url[id],
                    'town': town[0][24:-2],
                    'country': country[0]}
            if self.mongodb_save.count(data) <= 0:
                self.mongodb_save.insert(data)