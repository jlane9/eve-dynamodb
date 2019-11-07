"""
"""

import decimal
import itertools
from typing import Union
import boto3
from botocore.exceptions import ClientError as BotoCoreClientError
from bson import decimal128, ObjectId
from bson.dbref import DBRef
from eve.io.base import DataLayer
from eve.utils import ParsedRequest, config, debug_error_message, str_to_date
from flask import Flask, abort
import simplejson as json

from eve_dynamodb.expression import build_attr_expression, build_key_expression

"""
String/Set
Boolean
Null
Number/Set
Binary/Set
Map
List
"""


class DynamoDBResult:
    """DynamoDB search result
    """

    def __init__(self, result: dict, **_kwargs):
        """Initialize DynamoDB result

        :param dict result: DynamoDB response
        :param dict _kwargs: Extra arguments
        """

        self._result = result

    def __iter__(self):
        """Return next item from result

        :return:
        """

        if 'Items' not in self._result:
            return

        for item in self._result['Items']:
            yield item

    def count(self, **_kwargs) -> int:
        """Return a count of all items

        :param dict _kwargs: Extra arguments
        :return: Count of all items
        :rtype: int
        """

        return self._result['Count'] if 'Count' not in self._result else 0


class DynamoDB(DataLayer):
    """DynamoDB data layer access for Eve REST API
    """

    serializers = {
        'boolean': lambda v: {"1": True, "true": True, "0": False, "false": False}[str(v).lower()],
        'datetime': str_to_date,
        'dbref': lambda value: DBRef(
            value['$col'] if '$col' in value else value['$ref'],
            value['$id'],
            value['$db'] if '$db' in value else None
        ) if value is not None else None,
        'decimal': lambda value: decimal128.Decimal128(decimal.Decimal(str(value))) if value is not None else None,
        'float': lambda value: float(value) if value is not None else None,
        'integer': lambda value: int(value) if value is not None else None,
        'number': lambda val: json.loads(val) if val is not None else None,
        'objectid': lambda value: ObjectId(value) if value else None
    }

    def init_app(self, app: Flask):
        """Initialize DynamoDB

        :param Flask app: Flask application
        :return:
        """

        self.driver = boto3.resource('dynamodb')

    def find(self, resource: str, req: ParsedRequest = None, sub_resource_lookup: dict = None,
             perform_count: bool = True) -> tuple:
        """Retrieves a set of documents matching a given request

        :param str resource: Resource being accessed
        :param ParsedRequest req: Contains all the constraints that must be fulfilled in order to satisfy the request
        :param dict sub_resource_lookup: Sub-resource lookup from the endpoint url
        :param bool perform_count: Whether a document count should be performed and returned to the client
        :return: Result from DynamoDB search and count
        :rtype: tuple
        """

        args = dict()

        if req and req.max_results:
            args["limit"] = req.max_results

        if req and req.page > 1:
            args["skip"] = (req.page - 1) * req.max_results

        data_source, filter_, sort, projection = self.datasource(resource)

        try:
            table = self.driver.Table(data_source)

            # TODO: Finish this, return count
            return DynamoDBResult(table.scan()), None

        except BotoCoreClientError as e:
            abort(500, description=debug_error_message(e.response['Error']['Message']))

    def aggregate(self, resource: str, pipeline: dict, options: dict):
        """Perform an aggregation on the resource data source and returns the result

        :param str resource: Resource being accessed
        :param dict pipeline: Aggregation pipeline to be executed
        :param dict options: Aggregation options to be considered
        """

        # TODO: Finish this, maybe this isn't supported?. Also doc strings
        raise NotImplementedError

    def find_one(self, resource: str, req: ParsedRequest, check_auth_value: bool = True,
                 force_auth_field_projection: bool = False, **lookup) -> dict:
        """Retrieves a single document

        :param str resource: Resource being accessed
        :param ParsedRequest req: Contains all the constraints that must be fulfilled in order to satisfy the request
        :param bool check_auth_value: Boolean flag indicating if the find operation should consider user-restricted
        resource access. Defaults to ``True``
        :param bool force_auth_field_projection: Boolean flag indicating if the find operation should always include
        the user-restricted resource access field (if configured). Defaults to ``False``
        :param dict lookup: Lookup query
        :return: A single document
        :rtype: dict
        """

        client_projection = self._client_projection(req)
        is_soft_delete = config.DOMAIN[resource]["soft_delete"]
        show_deleted = req and req.show_deleted
        query_contains_deleted = self.query_contains_field(lookup, config.DELETED)

        data_source, filter_, projection, _ = self._datasource_ex(
            resource,
            lookup,
            client_projection,
            check_auth_value=check_auth_value,
            force_auth_field_projection=force_auth_field_projection,
        )

        if is_soft_delete and not show_deleted and not query_contains_deleted:
            filter_ = self.combine_queries(filter_, {config.DELETED: {"$ne": True}})

        try:
            table = self.driver.Table(data_source)
            result = table.get_item(Key=lookup)  # TODO: Add projection and pass filter, change to scan or query?
            return result['Item'] if 'Item' in result else None
        except BotoCoreClientError as e:
            abort(500, description=debug_error_message(e.response['Error']['Message']))

        """
        # Here, we feed pymongo with `None` if projection is empty.
        return (
            self.pymongo(resource).db[datasource].find_one(filter_, projection or None)
        )
        """

    def find_one_raw(self, resource: str, **lookup) -> dict:
        """Retrieves a single raw document

        :param str resource: Resource name
        :param dict lookup: Lookup query
        :return: A single document
        :rtype: dict
        """

        id_field = config.DOMAIN[resource]["id_field"]
        _id = lookup.get(id_field)
        data_source, filter_, _, _ = self._datasource_ex(resource, {id_field: _id}, None)

        try:
            table = self.driver.Table(data_source)
            result = table.get_item(Key=filter_)
            return result['Item'] if 'Item' in result else None
        except BotoCoreClientError as e:
            abort(500, description=debug_error_message(e.response['Error']['Message']))

    def find_list_of_ids(self, resource: str, ids: list, client_projection=None) -> DynamoDBResult:
        """Retrieves a list of documents from the collection given by `resource`, matching the given list of ids

        :param str resource: Resource name
        :param list ids: A list of ids corresponding to the documents to retrieve
        :param client_projection: A specific projection to use
        :return: A list of documents matching the ids in `ids` from the collection specified in `resource`
        :rtype: DynamoDBResult
        """

        id_field = config.DOMAIN[resource]["id_field"]
        query = {"$or": [{id_field: id_} for id_ in ids]}

        data_source, filter_, projection, _ = self._datasource_ex(
            resource, query=query, client_projection=client_projection
        )

        try:
            table = self.driver.Table(data_source)
            return DynamoDBResult(table.scan())  # TODO finish this
        except BotoCoreClientError as e:
            abort(500, description=debug_error_message(e.response['Error']['Message']))

    def insert(self, resource: str, doc_or_docs: Union[dict, list]) -> list:
        """Inserts a document into a resource collection/table

        :param str resource: Resource being accessed
        :param (Union[dict, list]) doc_or_docs: JSON document or list of JSON documents to be added to the database
        :return: A list of ids
        :rtype: list
        """

        id_field = config.DOMAIN[resource]["id_field"]
        data_source, _, _, _ = self._datasource_ex(resource)

        if isinstance(doc_or_docs, dict):
            doc_or_docs = [doc_or_docs]

        try:

            table = self.driver.Table(data_source)

            with table.batch_writer() as batch:
                for doc in doc_or_docs:
                    # Note: Existing documents are overwritten https://github.com/boto/boto/issues/3273
                    # TODO: Maybe we could a search first?
                    batch.put_item(Item=doc)

            return [doc[id_field] for doc in doc_or_docs]

        except BotoCoreClientError as e:
            abort(500, description=debug_error_message(e.response['Error']['Message']))

    def update(self, resource: str, id_: str, updates: dict, original: dict):
        """Updates a collection/table document/row
        :param str resource: Resource being accessed
        :param str id_: The unique id of the document
        :param dict updates: JSON updates to be performed on the database document (or row)
        :param dict original: Definition of the json document that should be updated
        :raise OriginalChangedError: Raised if the database layer notices a change from the supplied original parameter
        """

        # TODO: Finish this
        raise NotImplementedError

    def replace(self, resource: str, id_: str, document: dict, original: dict):
        """Replaces a collection/table document/row
        :param str resource: Resource being accessed
        :param str id_: The unique id of the document
        :param dict document: The new JSON document
        :param original: Definition of the json document that should be updated
        :raise OriginalChangedError: Raised if the database layer notices a change from the supplied original parameter
        """

        # TODO: Finish this
        raise NotImplementedError

    def remove(self, resource: str, lookup: dict):
        """Removes a document/row or an entire set of documents/rows from a database collection/table

        :param str resource: Resource being accessed
        :param dict lookup: A query that documents must match in order to qualify for deletion
        :return:
        """

        id_field = config.DOMAIN[resource]["id_field"]
        data_source, filter_, _, _ = self._datasource_ex(resource, lookup)

        try:
            table = self.driver.Table(data_source)

            with table.batch_writer() as batch:
                for item in self.find(resource, sub_resource_lookup=lookup):
                    batch.delete_item(Key={id_field: item[id_field]})

        except BotoCoreClientError as e:
            abort(500, description=debug_error_message(e.response['Error']['Message']))

    def combine_queries(self, query_a: dict, query_b: dict) -> dict:
        """Takes two db queries and applies db-specific syntax to produce the intersection

        :param dict query_a: Left query
        :param dict query_b: Right query
        :return: Combined query
        :rtype: dict
        """

        return {
            "$and": [
                {k: v} for k, v in itertools.chain(query_a.items(), query_b.items())
            ]
        }

    def get_value_from_query(self, query: dict, field_name: str) -> str:
        """For the specified field name, parses the query and returns the value being assigned in the query

        For example,
            get_value_from_query({'_id': 123}, '_id')
        123

        This mainly exists to deal with more complicated compound queries
            get_value_from_query(
                {'$and': [{'_id': 123}, {'firstname': 'mike'}],
                '_id'
            )
        123

        :param dict query: Lookup query
        :param str field_name: Field name to get value for
        :return: Value for field name within query
        :rtype: str
        """

        if field_name in query:
            return query[field_name]
        elif "$and" in query:
            for condition in query["$and"]:
                if field_name in condition:
                    return condition[field_name]
        raise KeyError

    def query_contains_field(self, query: dict, field_name: str) -> bool:
        """For the specified field name, does the query contain it?

        :param dict query: Filter query
        :param str field_name: Field name
        :return: True, if the query contains the field. False otherwise
        :rtype: bool
        """

        try:
            self.get_value_from_query(query, field_name)

        except KeyError:
            return False

        return True

    def is_empty(self, resource: str) -> bool:
        """Returns whether the resource is empty

        :param str resource: Resource being accessed
        :return: True, if the collection is empty. False otherwise
        :rtype: bool
        """

        data_source, filter_, _, _ = self.datasource(resource)

        try:
            table = self.driver.Table(data_source)

            if not filter_:
                return table.scan()['Count'] == 0
            else:

                if config.LAST_UPDATED in filter_:
                    del filter_[config.LAST_UPDATED]

                return self.find(resource, None, filter_, perform_count=True)[1] == 0

        except BotoCoreClientError as e:
            abort(400, description=debug_error_message(e.response['Error']['Message']))
