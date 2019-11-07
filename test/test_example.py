"""test_example
"""

from eve import Eve


def test_get_all(server: Eve):
    """Test to ensure dynamo can be queried for a records

    :param Eve server: Eve server
    :return:
    """

    with server.app_context():
        assert server.data.find('actor')
