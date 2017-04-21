__author__ = 'sunary'


from utils import my_helper
from utils.my_mongo import Mongodb


class AddTwitterKey():

    def __init__(self):
        self.twitter_key = [{'consumer_key':'QXN25hAuSIP3dyUZKgD4SktNc',
                        'consumer_secret': 'WS7NZfMcckB53BqVMNaRVZk3vvS3ohVKxGxLdYy6ssHS3JU0c8',
                        'access_token': '3249575378-LDdEUpmsmJT0dWg2ubOTqpqJuHivzOQi7L3n38w',
                        'access_token_secret': 'rB3vYHj6D4go7BrEM9ZwJRF7eevNlaoKFc8LXJe3VI6mn'
                        },
                            {'consumer_key':'Zeixof2Aay7UZXxlJBo76yppn',
                        'consumer_secret': 'qVyvSPFwzzj9wAnerwHvwzpGSrRhN0QUb3CuRR80G2oIMtUPGQ',
                        'access_token': '3249894415-gpWJ94lfeO2JzJVv4AGG45O0Uk5JCGSgKA6peJe',
                        'access_token_secret': 'E81gNt5nEFneM0fNGFbW18AHqCKUM46p9aleUSW0wgSZd'
                        }]

        self.mongo_key = Mongodb(host='ellytran.ssh.sentifi.com',
                                 port=27027,
                                 db='twitter',
                                 col='key')
        self.logger = my_helper.init_logger(self.__class__.__name__)

    def adds(self):
        self.logger.info('before add:' + str(self.mongo_key.count()))

        for key in self.twitter_key:
            data = {'key': key,
                    'active': True,
                    'group': 'nhat-v2',
                    'release_by': 'TweetCrawler',
                    'requester': 'TweetCrawler'}
            if self.mongo_key.count({'key': key}) <= 0:
                self.mongo_key.insert(data)

        self.logger.info('after add:' + str(self.mongo_key.count()))

if __name__ == '__main__':
    add_twitter_key = AddTwitterKey()
    add_twitter_key.adds()