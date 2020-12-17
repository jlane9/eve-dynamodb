"""test_find

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

from eve import Eve


def test_find_one_raw_by_id(server: Eve):
    """Test to ensure a single record can retrieved by id

    :param Eve server: Eve server
    :raises: AssertionError
    """

    with server.app_context():
        id_field = server.config['DOMAIN']['actor']['id_field']
        server.data.insert('actor', [{id_field: '1', 'fname': 'Oprah'}])
        actor = server.data.find_one_raw('actor', **{id_field: '1'})

        assert actor == {id_field: '1', 'fname': 'Oprah'}


def test_find_one_raw_only_by_id(server: Eve):
    """Test to ensure find_one_raw only uses _id for lookup

    :param Eve server: Eve server
    :raises: AssertionError
    """

    with server.app_context():

        id_field = server.config['DOMAIN']['actor']['id_field']
        server.data.insert('actor', [{id_field: '1', 'fname': 'Oprah'}])
        actor = server.data.find_one_raw('actor', **{id_field: '1', 'fname': 'Beyonce'})

        assert actor == {id_field: '1', 'fname': 'Oprah'}
