# -*- coding: utf-8 -*-


class Dispatcher(object):
    callbacks = []

    def register(self, callback):
        self.callbacks.append(callback)

    def unregister(self, callback):
        self.callbacks.remove(callback)

    def notifyCallbacks(self, msg):
        for callback in self.callbacks:
            callback(msg)

    def getMessages(self):
        return "No messages yet"
