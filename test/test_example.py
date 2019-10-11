"""
"""


def test_get_all(server):

    with server.app_context():
        actors = server.data.find('actor', None, None)
        assert actors
