__author__ = 'sunary'


from my_queue import Queue


class CountMessage():

    def __init__(self):
        self.sum_message = 0

    def count(self):
        self.sum_message += self.queue_tagging()
        self.sum_message += self.queue_language()
        self.sum_message += self.queue_semantic()
        self.sum_message += self.queue_score()

        return self.sum_message

    def queue_tagging(self):
        queue = Queue({
            'uri':['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
            'name':'TagTweetOrange_TweetTaggingTmp3'})
        size = queue.size()
        queue.close()
        return size

    def queue_language(self):
        queue = Queue({
            'uri':['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
            'name':'LangWorker_TweetTaggingScreenTmp'})
        size = queue.size()
        queue.close()
        return size

    def queue_semantic(self):
        queue = Queue({
            'uri':['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
            'name':'SemanticEventTwitterWorkerV3_TweetTaggingScreenTmp'})
        size = queue.size()
        queue.close()
        return size

    def queue_score(self):
        queue = Queue({
            'uri':['amqp://worker:onTheStage@rabbitmq-orange-01.ireland.sentifi.internal'],
            'name':'MessageScoringWorker_TweetTaggingScreenTmpV2'})
        size = queue.size()
        queue.close()
        return size

if __name__ == '__main__':
    count_message = CountMessage()
    print count_message.count()

