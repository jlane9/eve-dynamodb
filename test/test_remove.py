"""test_remove

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

from eve import Eve


def test_remove_one(server: Eve):
    """Test to ensure dynamo can remove a single record from a resource

    :param Eve server: Eve server
    :raises: AssertionError
    """

    with server.app_context():

        id_field = server.config['DOMAIN']['actor']['id_field']
        server.data.insert('actor', [{id_field: '1', 'fname': 'Oprah'}])
        server.data.remove('actor', {id_field: '1'})

        assert not server.data.find_one_raw('actor', **{id_field: '1'})
