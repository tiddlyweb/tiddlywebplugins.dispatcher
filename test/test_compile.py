


def test_compile():
    try:
        import tiddlywebplugins.dispatcher
        assert True
    except ImportError, exc:
        assert False, exc
