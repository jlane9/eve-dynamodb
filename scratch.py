from eve import Eve
from eve_dynamodb.dynamodb import DynamoDB

DOMAIN = {
    'actor': {
        'schema': {
            '_id': {'type': 'string', 'unique': True},
            'name': {'type': 'string'}
        },
        'datasource': {
            'backend': 'dynamodb'
        }
    },
}

settings = {'DOMAIN': DOMAIN}
app = Eve(settings=settings, data=DynamoDB)

with app.app_context():
    print(app.data.build_expression({'_id': '1'}, 'actor'))

