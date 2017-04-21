__author__ = 'sunary'


from utils import my_connection, my_helper, my_datetime
from bson.objectid import ObjectId
from queue.count_message_tmp import CountMessage
from automation.kpi_from_db import KPIFromDb
from utils.my_mongo import Mongodb


class DailyMessagesReport():

    def __init__(self):
        self.client_ES = my_connection.es_connection()
        self.logger = my_helper.init_logger(self.__class__.__name__)

    def orange_flow_message(self):
        tweet = Mongodb(host= 'mongo7.ireland.sentifi.internal',
                        db= 'sentifi',
                        col= 'tweet_potential_publisher')
        self.orange_flow = tweet.count({"_id": {"$gt": ObjectId(my_datetime.object_id_previous_days(days= 1)),
                                            "$lt": ObjectId(my_datetime.object_id_previous_days())}})
        self.logger.info('orange_flow_message: ' + str(self.orange_flow))

    def real_time_message(self):
        query = {
            "query": {
                "filtered": {
                   "filter": {
                       "range": {
                          "timestamp": {
                             "from": my_datetime.iso_previous_days(days= 1),
                             "to": my_datetime.iso_previous_days()
                          }
                       }
                   }
                }
            }
        }

        res = self.client_ES.count(index= "analytic_tmp",
                           doc_type= "relevant_document",
                           body= query,
                           request_timeout= 36000)
        self.real_time = res['count']
        self.logger.info('real_time_message: ' + str(self.real_time))

    def weekly_candidates_processed(self):
        query = {
           "query": {
              "filtered": {
                 "filter": {
                    "and": {
                       "filters": [
                          {
                             "range": {
                                "timestamp": {
                                    "from": my_datetime.iso_previous_days(7),
                                    "to": my_datetime.iso_previous_days()
                                }
                             }
                          },
                          {
                             "exists": {
                                "field": "score_long"
                             }
                          }
                       ]
                    }
                 }
              }
           },
           "aggs": {
              "distinct_candidates": {
                 "cardinality": {
                    "field": "publisher.twitterID"
                 }
              }
           }
        }

        res = self.client_ES.search(index= "analytic_tmp",
                            search_type= "count",
                            doc_type= "relevant_document",
                            body= query,
                            request_timeout= 36000)
        self.weekly_candidates = res['aggregations']['distinct_candidates']['value']
        self.logger.info('weekly_candidates_processed: ' + str(self.weekly_candidates))

    def total_candidates_processed(self):
        query = {
           "query": {
              "filtered": {
                 "filter": {
                    "and": {
                       "filters": [
                          {
                             "range": {
                                "timestamp": {
                                    "to": my_datetime.iso_previous_days()
                                }
                             }
                          },
                          {
                             "exists": {
                                "field": "score_long"
                             }
                          }
                       ]
                    }
                 }
              }
           },
           "aggs": {
              "distinct_candidates": {
                 "cardinality": {
                    "field": "publisher.twitterID"
                 }
              }
           }
        }

        res = self.client_ES.search(index= "analytic_tmp",
                            search_type= "count",
                            doc_type= "relevant_document",
                            body= query,
                            request_timeout= 36000)
        self.total_candidates = res['aggregations']['distinct_candidates']['value']
        self.logger.info('total_candidates_processed: ' + str(self.total_candidates))

    def messages_from_queue(self):
        count_messages = CountMessage()
        self.processing_messages = count_messages.count()
        self.logger.info('processing_messages: ' + str(self.processing_messages))

    def insert_db(self):
        con_dev = my_connection.dev_connection('nhat')

        list_id = [KPIFromDb.ID_TWEET_ORANGE_FLOW, KPIFromDb.ID_PROCESSING_MESSAGES, KPIFromDb.ID_CANDIDATES_PROCESSED]
        list_value = [self.orange_flow, self.processing_messages, self.total_candidates]

        cur_dev = con_dev.cursor()
        for i in range(len(list_id)):
            cur_dev.execute('''
                INSERT INTO public.kpi_report(id, created_at, value)
                VALUES(%s, now(), %s)
            ''', [list_id[i], list_value[i]])
            con_dev.commit()

        con_dev.close()

if __name__ == '__main__':
    daily_report = DailyMessagesReport()
    daily_report.orange_flow_message()
    # daily_report.real_time_message()
    daily_report.total_candidates_processed()
    daily_report.messages_from_queue()
    daily_report.insert_db()