"""
A listener for tiddlywebplugins.dispatcher.

A listener is started via a twanager command.
"""

import beanstalkc
import os
import logging

try:
    from multiprocessing import Process
except ImportError:
    from processing import Process

from tiddlyweb.manage import make_command

class Dispatcher(object):

    def __init__(self, config):
        self.config = config

    def run(self):
        beanstalk_host = self.config.get('beanstalk.host', 'localhost')
        beanstalk_port = self.config.get('beanstalk.port', 11300)
        beanstalk = beanstalkc.Connection(host=beanstalk_host,
            port=beanstalk_port)
        listeners = self.config.get('beanstalk.listeners', [__name__])
        queues = []
        for listener in listeners:
            listener_module = __import__(listener, {}, {}, ['Listener'])
            queue = listener_module.Listener.QUEUE
            listener_runner = listener_module.Listener(
                    kwargs={'queue': queue, 'config': self.config})
            listener_runner.daemon = True
            listener_runner.start()
            queues.append(queue)

        print 'I am ', os.getpid()
        while True:
            job = beanstalk.reserve()
            for queue in queues:
                beanstalk.use(queue)
                beanstalk.put(job.body)
            job.delete()


class Listener(Process):

    QUEUE = 'debug'

    def run(self):
        print dir(self)
        config = self._kwargs['config']
        queue = self._kwargs['queue']
        beanstalk_host = config.get('beanstalk.host', 'localhost')
        beanstalk_port = config.get('beanstalk.port', 11300)
        beanstalk = beanstalkc.Connection(host=beanstalk_host,
            port=beanstalk_port)
        beanstalk.watch(queue)
        beanstalk.ignore('default')
        logging.debug('using %s', beanstalk.using())
        self.beanstalk = beanstalk
        while True:
            job = self.beanstalk.reserve()
            print '%s i got a job, debugging %s' % (os.getpid(), job.body)
            job.delete()


def init(config):
    @make_command()
    def dispatcher(args):
        dispatcher = Dispatcher(config=config)
        # Doesn't exit
        dispatcher.run()
