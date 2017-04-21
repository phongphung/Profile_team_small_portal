__author__ = 'Stephen Huynh'

from pymongo import MongoClient


MONGO_HOST = 'dev.ssh.sentifi.com'
# MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'on_demand_crawling'
MONGO_COLLECTION = 'bloomberg_crawler'

def mongo_connection(host, port, db, collection):
    try:
        conn = MongoClient(host, port)
        return conn[db][collection]
    except:
        raise