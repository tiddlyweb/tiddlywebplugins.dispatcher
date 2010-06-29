"""
Initialize tiddlywebplugins.dispatcher by starting up the
necessary files.
"""

import beanstalkc

def init(config):
    beanstalk_host = config.get('beanstalk.host', 'localhost')
    beanstalk_port = config.get('beanstalk.port', 11300)
    config['beanstalkc'] = beanstalkc.Connection(host=beanstalk_host,
            port=beanstalk_port)
    _register_handler(config) 


def _handler(store, tiddler):
    """
    Inject the tiddler data into the default queue.
    """
    environ = store.environ
    beanstalkc = environ['tiddlyweb.config']['beanstalkc']
    try:
        username = environ['tiddlyweb.usersign']['name']
    except KeyError: 
        # Called from twanager.
        username = 'GUEST'
    # unicode concerns?
    #data = '%s\0%s\0%s\0%s' % ( username, tiddler.bag, tiddler.title, tiddler.revision)
    data = '%s%s%s%s' % ( username, tiddler.bag, tiddler.title, tiddler.revision)
    print data
    beanstalkc.put(data.encode('UTF-8'))


def _register_handler(config):
    """
    Ensure tiddler_written is properly hooked.
    """
    from tiddlyweb.stores import TIDDLER_WRITTEN_HANDLERS
    if _handler not in TIDDLER_WRITTEN_HANDLERS:
        TIDDLER_WRITTEN_HANDLERS.append(_handler)
