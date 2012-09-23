# -*- coding: utf-8 -*-

import tornado.escape
import tornado.web
import tornado.websocket
from tornado import httpclient

from uuid import uuid4


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        session = uuid4()
        msgs = self.application.dispatcher.getMessages()
        self.render("index.html", session=session, msgs=msgs,
            connected=self.application.rmanager.connected)


class ProducerHandler(tornado.web.RequestHandler):

    @tornado.web.asynchronous
    def get(self):
        http = httpclient.AsyncHTTPClient()
        http.fetch("http://friendfeed-api.com/v2/feed/bret",
            callback=self.on_response)

    def on_response(self, response):
        if response.error:
            raise tornado.web.HTTPError(500)

        json = tornado.escape.json_decode(response.body)

        com_len = 0
        for entries in json['entries']:
            if 'comments' in entries.keys():
                comments = entries['comments']
                com_len = com_len + len(comments)
                for comment in comments:
                    msg = comment['body'] + ' FROM: ' + comment['from']['name']
                    self.application.rmanager.publish(msg.encode("utf-8"))

        self.write("Fetched %d comments from the FriendFeed API" % com_len)
        self.finish()


class ConsumerHandler(tornado.websocket.WebSocketHandler):

    def open(self):
        self.application.dispatcher.register(self.callback)

    def on_close(self):
        self.application.dispatcher.unregister(self.callback)

    def on_message(self, message):
        pass

    def callback(self, msg):
        self.write_message('{"msg":"%s"}' % msg)
