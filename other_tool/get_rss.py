__author__ = 'sunary'


import urllib2
from xml.etree import ElementTree as etree
import re


class GetRSS():

    def __init__(self):
        self.list_rss = ['http://www.thestar.com.my/RSS/Business/Business-News',
                         'http://www.thestar.com.my/RSS/Business/Investing',
                         'http://www.thestar.com.my/RSS/Business/SME' ]

    def get_rss(self):
        rss_reader = urllib2.urlopen(self.list_rss[0])
        content = rss_reader.read()
        rss_reader.close()

        content = content.replace('content:encoded', 'content-encoded')

        #entire feed
        rss_tree = etree.fromstring(content)
        item = rss_tree.findall('channel/item')

        for entry in item:
            title = entry.findtext('title')
            link = entry.findtext('link')
            pub_date = entry.findtext('pubDate')
            content = entry.findtext('content-encoded')
            content = re.sub('<.+?>', '', content)

if __name__ == '__main__':
    get_rss = GetRSS()
    get_rss.get_rss()
