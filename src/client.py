# -*- coding: utf-8 -*-

import os
import logging

import pika
from pika.adapters.tornado_connection import TornadoConnection
from pika.reconnection_strategies import SimpleReconnectionStrategy

import tornado.httpserver
import tornado.ioloop
import tornado.web


class PikaClient(object):

    def __init__(self, config, app=None):
        # Connection params
        self.host = config['host'] or 'localhost'
        self.port = config['port'] or '5672'
        self.vhost = config['vhost'] or '/'
        self.user = config['user'] or 'guest'
        self.passwd = config['passwd'] or 'guest'
        self.exchange = config['exchange'] or 'myx'
        self.queue_name = config['queue_name'] or 'tornado-test-%i' \
            % os.getpid()
        self.routing_key = config['routing_key'] or 'tornado.*'

        # Default values
        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None

        self.app = app

        # Set our pika.log options
        pika.log.setup(color=True)

        # A place for us to keep messages sent to us by Rabbitmq
        self.messages = list()

        # A place for us to put pending messages while we're waiting to connect
        self.pending = list()

        self.connect()

    def connect(self):
        if self.connecting:
            pika.log.info('PikaClient: Already connecting to RabbitMQ')
            return
        pika.log.info('PikaClient: Connecting to RabbitMQ on %s:%i' \
            % (self.host, self.port))
        self.connecting = True

        credentials = pika.PlainCredentials(self.user, self.passwd)

        param = pika.ConnectionParameters(host=self.host, port=self.port,
            virtual_host=self.vhost, credentials=credentials)

        srs = SimpleReconnectionStrategy()

        logging.debug('Events: Connecting to AMQP Broker: %s:%i' % (self.host,
            self.port))

        # from pika.adapters import SelectConnection
        # connection = SelectConnection(parameters, on_connected)
        self.connection = TornadoConnection(param, reconnection_strategy=srs,
            on_open_callback=self.on_connected)

        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):
        pika.log.info('PikaClient: Connected to RabbitMQ on %s:%i' \
            % (self.host, self.port))

        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        pika.log.info('PikaClient: Channel Open, Declaring Exchange %s' \
            % self.exchange)

        self.channel = channel
        self.channel.exchange_declare(exchange=self.exchange,
                                      type="direct",
                                      auto_delete=True,
                                      durable=False,
                                      callback=self.on_exchange_declared)

    def on_exchange_declared(self, frame):
        pika.log.info('PikaClient: Exchange Declared, Declaring Queue %s' \
            % self.queue_name)
        self.channel.queue_declare(queue=self.queue_name,
                                   auto_delete=True,
                                   durable=False,
                                   exclusive=False,
                                   callback=self.on_queue_declared)

    def on_queue_declared(self, frame):
        pika.log.info('PikaClient: Queue Declared, Binding Queue')
        self.channel.queue_bind(exchange=self.exchange,
                                queue=self.queue_name,
                                routing_key=self.routing_key,
                                callback=self.on_queue_bound)

    def on_queue_bound(self, frame):
        pika.log.info('PikaClient: Queue Bound, Issuing Basic Consume')
        self.channel.basic_consume(consumer_callback=self.on_message,
                                   queue=self.queue_name,
                                   no_ack=True)
        # Send any messages pending
        for properties, body in self.pending:
            self.channel.basic_publish(exchange=self.exchange,
                                       routing_key=self.routing_key,
                                       body=body,
                                       properties=properties)

    def on_basic_cancel(self, frame):
        pika.log.info('PikaClient: Basic Cancel Ok')
        # If we don't have any more consumer processes running close
        self.connection.close()

    def on_closed(self, connection):
        # We've closed our pika connection so stop the demo
        tornado.ioloop.IOLoop.instance().stop()

    def on_message(self, channel, method, header, body):
        pika.log.info('PikaCient: Message received: %s delivery tag #%i: %s' \
           % (header.content_type, method.delivery_tag, body))

        # Append it to our messages list
        self.messages.append(body)

        self.app.dispatcher.notifyCallbacks(body)

    def get_messages(self):
        # Get the messages to return, then empty the list
        output = self.messages
        self.messages = list()
        return output

    def publish(self, msg):
        # Build a message to publish to RabbitMQ
        #body = '%.8f: Request from %s [%s]' % \
        #       (tornado_request._start_time,
        #        tornado_request.remote_ip,
        #        tornado_request.headers.get("User-Agent"))

        # Send the message
        properties = pika.BasicProperties(content_type="text/plain",
                                          delivery_mode=1)
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=self.routing_key,
                                   #body='Message: %s - %s' % (msg, body),
                                   body='Message: %s' % msg,
                                   properties=properties)
