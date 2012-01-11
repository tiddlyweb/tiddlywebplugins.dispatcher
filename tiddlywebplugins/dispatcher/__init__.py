"""
Initialize tiddlywebplugins.dispatcher by starting up the
necessary files.
"""

import beanstalkc
import logging

from tiddlywebplugins.dispatcher.listener import (
    DEFAULT_BEANSTALK_HOST, DEFAULT_BEANSTALK_PORT, BODY_SEPARATOR)

def init(config):
    """
    Ensure the main server has a connection to the beanstalkd.
    """
    beanstalk_host = config.get('beanstalk.host', DEFAULT_BEANSTALK_HOST)
    beanstalk_port = config.get('beanstalk.port', DEFAULT_BEANSTALK_PORT)
    config['beanstalkc'] = beanstalkc.Connection(host=beanstalk_host,
            port=beanstalk_port)
    _register_handler(config) 


def _handler(store, tiddler):
    """
    Inject the tiddler data into the default tube.
    """
    environ = store.environ
    beanstalk = environ['tiddlyweb.config']['beanstalkc']
    try:
        username = environ['tiddlyweb.usersign']['name']
    except KeyError: 
        # Called from twanager.
        username = 'GUEST'
    data = BODY_SEPARATOR.join([username, tiddler.bag, tiddler.title,
        str(tiddler.revision)])
    try:
        beanstalk.put(data.encode('UTF-8'))
    except beanstalkc.SocketError, exc:
        logging.error('unable to write to beanstalkd for %s:%s: %s',
                tiddler.bag, tiddler.title, exc)


def _register_handler(config):
    """
    Ensure writing a tiddler is properly hooked.
    """
    from tiddlyweb.store import HOOKS
    if _handler not in HOOKS['tiddler']['put']:
        HOOKS['tiddler']['put'].append(_handler)
    if _handler not in HOOKS['tiddler']['delete']:
        HOOKS['tiddler']['delete'].append(_handler)
