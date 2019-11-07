"""test_insert
"""

from eve import Eve


def test_insert(server: Eve):
    """Test to ensure records can be inserted into DynamoDB

    :param Eve server: Eve server
    :return:
    """

    with server.app_context():
        id_field = server.config['DOMAIN']['actor']['id_field']
        assert server.data.insert('actor', [{id_field: '1', 'fname': 'Oprah'}])
