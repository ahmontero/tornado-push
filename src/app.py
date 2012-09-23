# -*- coding: utf-8 -*-

import os.path

import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import tornado.httpserver
from tornado.options import define
from tornado.options import options

from client import PikaClient

from handlers import ProducerHandler
from handlers import ConsumerHandler
from handlers import IndexHandler
from dispatchers import Dispatcher


define("port", default=8888, help="run on the given port", type=int)


class Application(tornado.web.Application):

    def __init__(self):
        self.dispatcher = Dispatcher()
        self.rmanager = None

        pika_settings = dict(
            host="localhost",
            port=5672,
            vhost="/",
            user="guest",
            passwd="guest",
            exchange="tornado",
            queue_name="tornado-test-%i" % os.getpid(),
            routing_key="tornado.*"
        )

        self.rmanager = PikaClient(pika_settings, self)

        handlers = [
            (r"/", IndexHandler),
            (r"/producer", ProducerHandler),
            (r"/consumer", ConsumerHandler),
        ]

        settings = dict(
            cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=True,
            autoescape=None,
        )

        tornado.web.Application.__init__(self, handlers, **settings)


def main():
    tornado.options.parse_command_line()
    app = Application()

    server = tornado.httpserver.HTTPServer(app)
    server.listen(options.port)

    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
