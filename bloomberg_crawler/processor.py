import sys
import logging
from unicodecsv import DictReader
from scrapy.crawler import CrawlerProcess
from bloomberg_crawler.spiders.bloomberg_spider\
    import BloombergSpider
from bloomberg_crawler import db as temp_company_db
import csv_exporter


class BloombergCrawler(object):
    def read_csv_input_file(self, file_name):
        tickers = []
        try:
            with open(file_name, 'r') as f:
                reader = DictReader(f, delimiter=',')
                for line in reader:
                    current_ticker = dict(
                            Name=line['Name'],
                            Exchange=line['Exchange'],
                            root_ticker=line['root_ticker'],
                            Bloomberg_ticker=line['Bloomberg_ticker']
                    )
                    tickers.append(current_ticker)
        except:
            logging.error("Can not read the input file.")
            raise

        return tickers

    # if __name__ == "__main__":

    def process(self, input_file, output_file):
        # Check if has sufficient number of argument
        # assert len(sys.argv) >= 3

        # Drop old version of temporary temp_db
        temp_company_db.mongo_connection(temp_company_db.MONGO_HOST, temp_company_db.MONGO_PORT,
                                         temp_company_db.MONGO_DB, temp_company_db.MONGO_COLLECTION).remove()

        # Read input file with filename from terminal
        tickers = self.read_csv_input_file(input_file)
        print(tickers)

        # Crawl data
        try:
            process = CrawlerProcess()
            process.crawl(BloombergSpider, tickers)
            process.start()
        except:
            logging.error('Can not crawl the data.')
            raise

        # Export csv file
        csv_exporter.export_csv_output_file(output_file)
