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


BODY_SEPARATOR = '\0'
BODY_PACK_FIELDS = ['user', 'bag', 'tiddler', 'revision']
DEFAULT_BEANSTALK_HOST = 'localhost'
DEFAULT_BEANSTALK_PORT = 11300

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
        beanstalk = beanstalkc.Connection(host=beanstalk_host,
            port=beanstalk_port)
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

        while True:
            job = beanstalk.reserve()
            for tube in tubes:
                beanstalk.use(tube)
                beanstalk.put(job.body)
            job.delete()


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
        beanstalk = beanstalkc.Connection(host=beanstalk_host,
            port=beanstalk_port)
        beanstalk.watch(tube)
        beanstalk.ignore('default')
        logging.debug('using %s', beanstalk.using())
        self.config = config
        while True:
            job = beanstalk.reserve()
            self._act(job)
            job.delete()

    def _act(self, job):
        print '%s i got a job, debugging %s' % (os.getpid(),
                self._unpack(job))

    def _unpack(self, job):
        info_items = [item.decode('UTF-8') for item in job.body.split('\0')]
        return dict(zip(BODY_PACK_FIELDS, info_items))


def init(config):
    @make_command()
    def dispatcher(args):
        dispatcher = Dispatcher(config=config)
        # Doesn't exit
        dispatcher.run()
