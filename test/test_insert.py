"""test_insert
"""

from eve import Eve


def test_insert(server: Eve):
    """Test to ensure records can be inserted into DynamoDB

    :param Eve server: Eve server
    :raises: AssertionError
    """

    with server.app_context():
        id_field = server.config['DOMAIN']['actor']['id_field']
        assert server.data.insert('actor', [{id_field: '1', 'fname': 'Oprah'}])


def test_duplicate_insert(server: Eve):
    """Test to ensure id must remain unique

    :param Eve server: Eve server
    :raises: AssertionError
    """

    with server.app_context():
        id_field = server.config['DOMAIN']['actor']['id_field']
        server.data.insert('actor', [{id_field: '1', 'fname': 'Oprah'}])
        server.data.insert('actor', [{id_field: '1', 'fname': 'Kanye'}])
