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

table = app.data.driver.Table('actor')
doc_or_docs = [{'_id': '4', 'fname': 'String'}, {'_id': '6', 'fname': 'Flea'}]

with app.app_context():
    app.data.insert('actor', doc_or_docs)
