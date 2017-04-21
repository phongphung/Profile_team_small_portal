import json
import logging
import random
from time import sleep
import traceback
import amqp
import kombu
import kombu.mixins


class Message():
    def __init__(self):
        pass

    body = None
    tag = None


class QueueConnection:
    def __init__(self, config, logger=None):
        self.config = config
        self.connection = None
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        broker_url = []
        if type(config) is dict:
            uri_config = None
            if config.get('uri') is not None:
                uri_config = config.get('uri')
            elif config.get('Uri') is not None:
                uri_config = config.get('Uri')

            if uri_config is not None:
                uris = uri_config.split(',')

                # Random the order of uris to share the load for the first server
                random.shuffle(uris)

                for uri in uris:
                    broker_url.append('amqp://%s:5672/' % uri)
            else:
                broker_url.append('amqp://{0}:{1}@{2}:5672/'.format(config['username'], config['password'], config['host']))
        elif type(config) is list:
            for uri in config:
                broker_url.append('amqp://%s:5672/' % uri)

            print self.connection

        if len(broker_url) == 0:
            raise Exception("Could not parse queue connection config. %s" % json.dumps(config))

        self.broker_url = broker_url
        self.connect()

    def connect(self):
        uri = ";".join(self.broker_url)
        self.logger.debug('Connect to %s' % uri)
        self.connection = kombu.connection.Connection(uri, failover_strategy='shuffle')
        self.connection.ensure_connection()
        self.connection.connect()

    def reconnect(self):
        try:
            self.logger.debug('Release connection')
            self.connection.release()
        except Exception as e:
            self.logger.debug(traceback.format_exc(e))

        # Connect
        self.connect()

    def channel(self):
        return self.connection.channel()

    def re_init_channel(self, channel):
        try:
            self.logger.debug('Close current channel')
            channel.close()
        except Exception as e:
            self.logger.debug(traceback.format_exc(e))

        while True:
            try:
                sleep(10)
                self.logger.debug('Sleep 10s before re-init')

                self.logger.debug('Re-connect')
                self.reconnect()

                self.logger.debug('Open new channel')
                return self.channel()
            except Exception as e:
                self.logger.debug(traceback.format_exc(e))

    def release(self):
        self.connection.release()


class Queue():
    """
    Queue wrapper
    """

    def __init__(self, config, queue_name=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.debug('Init queue')
        self.queue_connection = QueueConnection(config)

        self.channel = self.queue_connection.channel()
        if queue_name is None:
            queue_name = config.get('Name')

        assert queue_name is not None

        self.queue_name = queue_name
        self.is_exchange = queue_name.endswith('Exchange')
        self.no_ack = False
        self.simple_queue = None

    def receive(self):
        """
        Receive a message from queue
        :return:
        """
        try:
            response = self.channel.basic_get(self.queue_name, no_ack=self.no_ack)
        except Exception as e:
            self.logger.error('Receive message error')
            self.logger.debug(traceback.format_exc(e))

            self.channel = self.queue_connection.re_init_channel(self.channel)

            response = None

        if response is None:
            return None

        message = Message()
        message.body = response.body
        message.tag = response

        return message

    def delete(self, message):
        """
        Delete message

        :param message:
        :return:
        """
        while True:
            try:
                self.channel.basic_ack(message.tag.delivery_tag)
            except Exception as e:
                self.logger.error('Delete message error')
                self.logger.debug(traceback.format_exc(e))

                self.channel = self.queue_connection.re_init_channel(self.channel)

    def reject(self, message):
        """
        Reject message

        :param message:
        :return:
        """
        while True:
            try:
                self.channel.basic_reject(message.tag.delivery_tag, True)
                break
            except Exception as e:
                self.logger.error('Reject message error')
                self.logger.debug(traceback.format_exc(e))

                self.channel = self.queue_connection.re_init_channel(self.channel)

    def post(self, text):
        """
        Post message to an exchange
        :param text:
        :return:
        """
        if not self.is_exchange:
            return self.post_simple_queue(text)

        message = amqp.Message(text)
        while True:
            try:
                self.logger.debug('Post to %s: %s' % (self.queue_name, text))
                self.channel.basic_publish(msg=message, exchange=self.queue_name)
                break
            except Exception as e:
                self.logger.debug('Post message to queue failed with exception')
                self.logger.debug(traceback.format_exc(e))

                self.channel = self.queue_connection.re_init_channel(self.channel)

        return True

    def post_simple_queue(self, text):
        """
        Using SimpleQueue to post directly to queue
        :param text:
        :return:
        """
        while True:
            try:
                simple_queue = self.get_simple_queue()
                simple_queue.put(text)
                break
            except Exception as e:
                self.logger.error('Post message to simple_queue failed with exception')
                self.logger.exception(e)

                try:
                    if self.simple_queue is not None:
                        self.simple_queue.close()
                except Exception, e:
                    self.logger.error('Close simple_queue error')
                    self.logger.exception(e)

                self.logger.info('Sleep 10s before re-connect')
                sleep(10)

                self.logger.info('Re-connect')
                self.queue_connection.reconnect()

                self.logger.info('Re-Init simple_queue')
                self.init_simple_queue()

        return True

    def get_simple_queue(self):
        if self.simple_queue is None:
            self.init_simple_queue()

        return self.simple_queue

    def init_simple_queue(self):
        self.simple_queue = self.queue_connection.connection.SimpleQueue(self.queue_name)

    def get_size(self):
        if self.is_exchange:
            self.logger.warn('Size of Exchange is always 0')
            return 0

        while True:
            try:
                queue_info = self.channel.queue_declare(queue=self.queue_name, passive=True)

                return queue_info.message_count
            except Exception as e:
                self.logger.error('Get queue size failed with exception')
                self.logger.exception(e)

                self.channel = self.queue_connection.re_init_channel(self.channel)

    def close(self):
        try:
            if self.simple_queue is not None:
                self.simple_queue.close()
        except Exception as e:
            self.logger.debug(traceback.format_exc(e))

        try:
            self.channel.close()
        except Exception as e:
            self.logger.debug(traceback.format_exc(e))

        try:
            self.queue_connection.release()
        except Exception as e:
            self.logger.debug(traceback.format_exc(e))


class ConsumerQueue(kombu.mixins.ConsumerMixin):
    def __init__(self, config, queue_name):
        self.queue_connection = QueueConnection(config)
        self.queue_connection.connect()

        self.connection = self.queue_connection.connection

        self.queue_name = queue_name
        self.is_exchange = queue_name.endswith('Exchange')
        self.no_ack = False
        self.queue = kombu.Queue(queue_name)
        self.on_message = None
        if type(config) is dict and config.get('prefetch_count') is not None:
            self.prefetch_count = config.get('prefetch_count')
        else:
            self.prefetch_count = 100

    def get_consumers(self, Consumer, channel):
        c = Consumer(queues=[self.queue], callbacks=[self.on_message])
        # Not sure exactly prefetch_size is, but let try to improve the performance
        # c.qos(prefetch_count=self.prefetch_count, prefetch_size=self.prefetch_count)
        c.qos(prefetch_count=self.prefetch_count)
        return [
            c,
        ]