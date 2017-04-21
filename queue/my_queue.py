__author__ = 'sunary'


import amqp
import kombu
import kombu.mixins
import time


class Message():
    def __init__(self):
        self.body = None
        self.tag = None


class QueueConnection():

    def __init__(self, uri):
        self.uri = uri
        self.connect()

    def connect(self):
        self._connection = kombu.connection.Connection(self.uri, failover_strategy='shuffle')
        self._connection.ensure_connection()
        self._connection.connect()

    def reconnect(self):
        try:
            self.release()
        except:
            pass

        self.connect()

    def channel(self):
        return self._connection.channel()

    def re_channel(self, channel):
        try:
            channel.close()
        except:
            pass

        while True:
            try:
                time.sleep(10)
                self.reconnect()
                return self.channel()
            except:
                pass

    def release(self):
        self._connection.release()


class Queue():

    def __init__(self, config):
        '''
        Examples:
            >>> Queue({
            ... 'uri': ['amqp://username:password@host:5672/'],
            ... 'name': 'ExampleQueue'
            ... })
        '''
        self.queue_name = config.get('name')
        self.is_exchange = self.queue_name.endswith('Exchange')
        self.queue_connection = QueueConnection(';'.join(config.get('uri')))
        self.channel = self.queue_connection.channel()
        self.simple_queue = None

    def receiver(self):
        try:
            response = self.channel.basic_get(self.queue_name, no_ack=False)
        except:
            self.channel = self.queue_connection.re_channel(self.channel)
            return None

        message = Message()
        message.body = response.body
        message.tag = response

        return message

    def delete(self, message):
        while True:
            try:
                self.channel.basic_ack(message.tag.delivery_tag)
            except:
                self.channel = self.queue_connection.re_channel(self.channel)

    def reject(self, message):
        while True:
            try:
                self.channel.basic_reject(message.tag.delivery_tag, True)
            except:
                self.channel = self.queue_connection.re_channel(self.channel)

    def post(self, payload):
        if self.is_exchange:
            message = amqp.Message(payload)
            while True:
                try:
                    self.channel.basic_publish(msg=message, exchange=self.queue_name)
                    break
                except:
                    self.channel = self.queue_connection.re_channel(self.channel)
        else:
             while True:
                try:
                    if not self.simple_queue:
                        self.simple_queue = self.queue_connection._connection.SimpleQueue(self.queue_name)

                    self.simple_queue.put(payload)
                    break
                except:
                    self.queue_connection.reconnect()
                    if self.simple_queue:
                        self.simple_queue.close()

    def size(self):
        if self.is_exchange:
            return 0

        while True:
            try:
                queue_info = self.channel.queue_declare(queue=self.queue_name, passive=True)
                return queue_info.message_count
            except Exception, e:
                self.channel = self.queue_connection.re_channel(self.channel)

    def close(self):
        try:
            if self.simple_queue is not None:
                self.simple_queue.close()
        except:
            pass

        try:
            self.channel.close()
        except:
            pass

        try:
            self.queue_connection.release()
        except:
            pass

class ConsumerQueue(kombu.mixins.ConsumerMixin):
    def __init__(self, config):
        '''
        Examples:
            >>> ConsumerQueue({
            ... 'uri': ['amqp://username:password@host:5672/'],
            ... 'name': 'ExampleQueue',
            ... 'prefetch_count': 100
            ... })
        '''
        self.queue_connection = QueueConnection(';'.join(config.get('uri')))
        self.queue = kombu.Queue(config.get('name'))
        self.on_message = None

        self.prefetch_count = config.get('prefetch_count', 100)

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[self.queue], callbacks=[self.on_message])
        consumer.qos(prefetch_count=self.prefetch_count)
        return [consumer,]