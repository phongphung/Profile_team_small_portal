__author__ = 'sunary'


from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy.utils.project import get_project_settings
from football_club_crawling import AutoRobot as AutoRobot


class ReactorControl:

    def __init__(self):
        self.crawlers_running = 0

    def add_crawler(self):
        self.crawlers_running += 1

    def remove_crawler(self):
        self.crawlers_running -= 1
        if self.crawlers_running == 0:
            reactor.stop()

def setup_crawler():
    reactor_control = ReactorControl()
    settings = get_project_settings()

    crawler = Crawler(settings)
    crawler.configure()
    crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
    spider = AutoRobot()
    crawler.crawl(spider)
    reactor_control.add_crawler()
    crawler.start()


if __name__ == '__main__':
    setup_crawler()
    reactor.run()