# _*_ coding: utf-8 _*_
__author__ = 'H6'

import logging
from bs4 import BeautifulSoup
from scrapy import Request
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider
from bloomberg_crawler import db as log_db


class BloombergSpider(CrawlSpider):
    name = 'bloomberg'
    allowed_domains = ['bloomberg.com']

    custom_settings = {
        'BOT_NAME': 'bloomberg_scraper',
        'DEPTH-LIMIT': 7,
    }

    def __init__(self, tickers):
        self.tickers = tickers

    def start_requests(self):
        URL_PATTERN = 'http://www.bloomberg.com/quote/%s'

        for ticker in self.tickers:
            url = URL_PATTERN % ticker.get('Bloomberg_ticker', None)
            USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) "
            "AppleWebKit/537.1 (KHTML, like Gecko)"
            " Chrome/22.0.1207.1 Safari/537.1"

            yield Request(url, headers={'User-Agent': USER_AGENT},
                          dont_filter=True,
                          meta={"ticker": ticker, "dont_redirect": True},
                          callback=self.parse_save_company)

    def parse_save_company(self, response):
        """
        Receive the response then parse items and store into MongoDB
        :param response: Respone from Http Request
        :return:
        """
        if response.status == 200:
            selector = Selector(text=response.body)

            # XPATH of the required fields
            name_xpath = "//div[@class='quote-page module']" \
                         "/div[@class='basic-quote']/div/h1[@class='name']/text()"
            exchange_xpath = "//div[@class='quote-page module']" \
                             "/div[@class='basic-quote']/div" \
                             "/div[@class='ticker-container']" \
                             "/div[@class='exchange']/text()"
            description_xpath = "//div[@class='bottom-panels']" \
                            "/div[@class='profile show']/div" \
                            "/div[@class='profile__description']/text()"
            address_xpath = "//div[@class='profile__details ']" \
                            "/div[@class='profile__detail profile__detail__address']"
            website_xpath = "//div[@class='profile__detail']" \
                            "/a[@class='profile__detail__website_link']/text()"
            executives_xpath = "//section[@id='executives']/div" \
                               "/ul[@class='management__executives']/li" \
                               "/span/a/@title"
            sector_xpath = "//div[@class='data-table data-table_detailed']/div[14]" \
                           "/div[@class='cell__value cell__value_text']/text()"
            industry_xpath = "//div[@class='data-table data-table_detailed']/div[15]" \
                             "/div[@class='cell__value cell__value_text']/text()"
            sub_industry_xpath = "//div[@class='data-table data-table_detailed']" \
                                 "/div[16]/div[@class='cell__value cell__value_text']" \
                                 "/text()"

            # Extract fields from XPATH, pass into a dict
            fields_extraction = dict(
                    B_Name=selector.xpath(name_xpath).extract(),
                    B_exchange=selector.xpath(exchange_xpath).extract(),
                    B_description=selector.xpath(description_xpath).extract(),
                    B_Address=selector.xpath(address_xpath).extract(),
                    B_url=selector.xpath(website_xpath).extract(),
                    B_executives=selector.xpath(executives_xpath).extract(),
                    B_Sector=selector.xpath(sector_xpath).extract(),
                    B_Industry=selector.xpath(industry_xpath).extract(),
                    B_Sub_industry=selector.xpath(sub_industry_xpath).extract()
            )

            # Parse the text
            ticker_data = response.meta['ticker']
            for field_name, field_value in fields_extraction.iteritems():
                if field_value:
                    if field_name == "B_Address":
                        bs = BeautifulSoup(field_value[0])
                        ticker_data[field_name] = bs.find_all(text=True)[2:]
                        print ticker_data[field_name]
                    elif field_name == "B_executives":
                        ticker_data[field_name] = []
                        for people in field_value:
                            ticker_data[field_name].append(people)
                    else:
                        bs = BeautifulSoup(field_value[0])
                        ticker_data[field_name] = bs.get_text().strip()
                else:
                    ticker_data[field_name] = None

            # Normalize the address and executives:
            if ticker_data["B_Address"]:
                ticker_data["B_Address"] = [item.strip() for item in ticker_data["B_Address"]]
                ticker_data["B_Address"] = " ".join(ticker_data["B_Address"])
            if ticker_data["B_executives"]:
                ticker_data["B_executives"] = '\n'.join(ticker_data["B_executives"])

            # Store into mongodb
            insert_article = log_db.mongo_connection(log_db.MONGO_HOST, log_db.MONGO_PORT,
                                                     log_db.MONGO_DB, log_db.MONGO_COLLECTION) \
                .save(
                    {
                        "_id": ticker_data['Bloomberg_ticker'],
                        "data": ticker_data
                    }
            )
        else:
            logging.error("cannot extract the name for %s" % response.meta["ticker"])