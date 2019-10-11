"""
"""


import eve
import pytest
from eve_dynamodb.dynamodb import DynamoDB


@pytest.fixture(scope="session")
def server():

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
