"""
Initialize tiddlywebplugins.dispatcher by starting up the
necessary files.
"""

import beanstalkc

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
    beanstalkc = environ['tiddlyweb.config']['beanstalkc']
    try:
        username = environ['tiddlyweb.usersign']['name']
    except KeyError: 
        # Called from twanager.
        username = 'GUEST'
    data = BODY_SEPARATOR.join([username, tiddler.bag, tiddler.title,
        str(tiddler.revision)])
    beanstalkc.put(data.encode('UTF-8'))


def _register_handler(config):
    """
    Ensure tiddler_written is properly hooked.
    """
    from tiddlyweb.stores import TIDDLER_WRITTEN_HANDLERS
    if _handler not in TIDDLER_WRITTEN_HANDLERS:
        TIDDLER_WRITTEN_HANDLERS.append(_handler)
