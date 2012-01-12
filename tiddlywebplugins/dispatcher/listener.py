"""
A listener for tiddlywebplugins.dispatcher.

A listener is started via a twanager command.
"""

import beanstalkc
import os
import sys
import logging

try:
    from multiprocessing import Process
except ImportError:
    from processing import Process

from tiddlyweb.manage import make_command
from tiddlywebplugins.dispatcher import (DEFAULT_BEANSTALK_HOST,
        DEFAULT_BEANSTALK_PORT, BODY_PACK_FIELDS, BODY_SEPARATOR,
        make_beanstalkc)


class Dispatcher(object):

    def __init__(self, config):
        self.config = config

    def run(self):
        """
        Run forever, listening on the 'default' tube. When a message
        is received, send it down all the registered non-default tubes.
        """
        beanstalk_host = self.config.get('beanstalk.host',
                DEFAULT_BEANSTALK_HOST)
        beanstalk_port = self.config.get('beanstalk.port',
                DEFAULT_BEANSTALK_PORT)
        beanstalk = make_beanstalkc(beanstalk_host, beanstalk_port)
        listeners = self.config.get('beanstalk.listeners', [__name__])

        tubes = []
        for listener in listeners:
            listener_module = __import__(listener, {}, {}, ['Listener'])
            tube = listener_module.Listener.TUBE
            listener_runner = listener_module.Listener(
                    kwargs={'tube': tube, 'config': self.config})
            listener_runner.daemon = True
            listener_runner.start()
            tubes.append(tube)

        try:
            while True:
                job = beanstalk.reserve()
                for tube in tubes:
                    beanstalk.use(tube)
                    beanstalk.put(job.body)
                job.delete()
        except beanstalkc.SocketError, exc:
            # retry on new client
            logging.error('dispatcher error reading beanstalk, restart: %s',
                    exc)
            self.run()
        except KeyboardInterrupt:
            logging.debug('dispatcher exiting on keyboard interrupt')
            sys.exit(0)


class Listener(Process):
    """
    A listener process that listens on a specific beanstalk tube.

    When a message is received as a job, self_act is called.
    """

    TUBE = 'debug'

    def run(self):
        config = self._kwargs['config']
        tube = self._kwargs['tube']
        beanstalk_host = config.get('beanstalk.host', DEFAULT_BEANSTALK_HOST)
        beanstalk_port = config.get('beanstalk.port', DEFAULT_BEANSTALK_PORT)
        beanstalk = make_beanstalkc(beanstalk_host, beanstalk_port)
        beanstalk.watch(tube)
        beanstalk.ignore('default')
        logging.debug('using %s', beanstalk.using())
        self.config = config
        try:
            while True:
                job = beanstalk.reserve()
                self._act(job)
                job.delete()
        except beanstalkc.SocketError, exc:
            # retry on new client
            logging.error('listener error reading beanstalk, restart: %s',
                    exc)
            self.run()
        except KeyboardInterrupt:
            logging.debug('listener on %s tube exiting on keyboard interrupt',
                    self.TUBE)
            sys.exit(0)

    def _act(self, job):
        print '%s i got a job, debugging %s' % (os.getpid(),
                self._unpack(job))

    def _unpack(self, job):
        info_items = [item.decode('UTF-8')
                for item in job.body.split(BODY_SEPARATOR)]
        return dict(zip(BODY_PACK_FIELDS, info_items))


def init(config):
    @make_command()
    def dispatcher(args):
        dispatcher = Dispatcher(config=config)
        # Doesn't exit
        dispatcher.run()
