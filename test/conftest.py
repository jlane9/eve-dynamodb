"""conftest

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""


import eve
import pytest
from eve_dynamodb.dynamodb import DynamoDB


@pytest.fixture(scope="session")
def server():
    """Returns an Eve server instance

    :return: Eve server
    :rtype: eve.Eve
    """

    settings = {
        'DOMAIN': {
            'actor': {
                'schema': {
                    '_id': {'type': 'string', 'unique': True},
                    'name': {'type': 'string'}
                }
            }
        }
    }

    return eve.Eve(settings=settings, data=DynamoDB)
