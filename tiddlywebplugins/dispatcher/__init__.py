"""
Initialize tiddlywebplugins.dispatcher by starting up the
necessary files.
"""

import beanstalkc
import logging
from time import sleep


BODY_SEPARATOR = '\0'
BODY_PACK_FIELDS = ['user', 'bag', 'tiddler', 'revision']
DEFAULT_BEANSTALK_HOST = 'localhost'
DEFAULT_BEANSTALK_PORT = 11300


def init(config):
    """
    Ensure the main server has a connection to the beanstalkd.
    """
    make_provider_beanstalkc(config, bail=True)
    _register_handler()


def make_provider_beanstalkc(config=None, bail=True):
    """
    Make connection to beanstalkd for use for injecting jobs
    from core tiddlyweb. Failures should not linger, so retry
    only once.
    """
    if config is None:
        config = {}
    beanstalk_host = config.get('beanstalk.host', DEFAULT_BEANSTALK_HOST)
    beanstalk_port = config.get('beanstalk.port', DEFAULT_BEANSTALK_PORT)
    client = make_beanstalkc(beanstalk_host, beanstalk_port, bail)
    config['beanstalkc'] = client


def make_beanstalkc(host, port, bail=False, backoff=1):
    """
    Make connection to beanstalkd. Default to trying a psuedo-exponential
    retry backoff scheme, up to a half second.
    """
    try:
        client = beanstalkc.Connection(host=host, port=port)
        logging.debug('making new connection to beanstalkc')
    except beanstalkc.SocketError, exc:
        logging.error('unable to make beanstalkc @%s connection: %s',
                backoff, exc)
        if bail:
            raise
        else:
            if backoff > 512:
                raise
            sleep(backoff / 10.0)  # wait some number ms
            backoff = backoff * 2
            client = make_beanstalkc(host, port, bail=bail, backoff=backoff)
    return client


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
        try:  # same again, but only once, this job is dropped if no second
            make_provider_beanstalkc(environ['tiddlyweb.config'])
            _handler(store, tiddler)
        except beanstalkc.SocketError, exc:
            logging.error('unable to reconnect to beanstalk: %s', exc)


def _register_handler():
    """
    Ensure writing a tiddler is properly hooked.
    """
    from tiddlyweb.store import HOOKS
    if _handler not in HOOKS['tiddler']['put']:
        HOOKS['tiddler']['put'].append(_handler)
    if _handler not in HOOKS['tiddler']['delete']:
        HOOKS['tiddler']['delete'].append(_handler)
