"""
"""

import boto3
from botocore.exceptions import ClientError as BotoCoreClientError
from eve.io.base import DataLayer
from eve.utils import config, debug_error_message, str_to_date
from flask import abort

from eve_dynamodb.expression import build_expression

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

    def __init__(self, result, **_kwargs):
        self._result = result

    def __iter__(self):

        if 'Items' not in self._result:
            return

        for item in self._result['Items']:
            yield item

    def count(self, with_limit_and_skip=False, **_kwargs):

        # TODO: Figure out what this is?
        if with_limit_and_skip:
            raise NotImplementedError

        return self._result['Count'] if 'Count' not in self._result else 0


class DynamoDB(DataLayer):
    """DynamoDB data layer access for Eve REST API
    """

    serializers = {
        'datetime': str_to_date,
        'integer': lambda value: int(value) if value is not None else None,
        'float': lambda value: float(value) if value is not None else None,
        'boolean': lambda v: {"1": True, "true": True, "0": False, "false": False}[str(v).lower()]
    }

    def init_app(self, app):
        """Initialize DynamoDB

        :param app:
        :return:
        """

        self.driver = boto3.resource('dynamodb')

    def find(self, resource, req, sub_resource_lookup, perform_count=True):
        """ Retrieves a set of documents (rows), matching the current request.
        Consumed when a request hits a collection/document endpoint
        (`/people/`).

        :param resource: resource being accessed. You should then use
                         the ``datasource`` helper function to retrieve both
                         the db collection/table and base query (filter), if
                         any.
        :param req: an instance of ``eve.utils.ParsedRequest``. This contains
                    all the constraints that must be fulfilled in order to
                    satisfy the original request (where and sort parts, paging,
                    etc). Be warned that `where` and `sort` expressions will
                    need proper parsing, according to the syntax that you want
                    to support with your driver. For example ``eve.io.Mongo``
                    supports both Python and Mongo-like query syntaxes.
        :param sub_resource_lookup: sub-resource lookup from the endpoint url.
        :param perform_count: whether a document count should be performed and
                              returned to the client.
        """

        data_source, filter_, sort, projection = self.datasource(resource)

        try:
            table = self.driver.Table(data_source)
            return DynamoDBResult(table.scan())
        except BotoCoreClientError as e:
            abort(400, description=debug_error_message(e.response['Error']['Message']))

    def aggregate(self, resource, pipeline, options):
        """ Perform an aggregation on the resource datasource and returns
        the result. Only implent this if the underlying db engine supports
        aggregation operations.

        :param resource: resource being accessed. You should then use
                         the ``datasource`` helper function to retrieve
                         the db collection/table consumed by the resource.
        :param pipeline: aggregation pipeline to be executed.
        :param options: aggregation options to be considered.
        """

        raise NotImplementedError

    def find_one(self, resource, req, check_auth_value=True, force_auth_field_projection=False, **lookup):
        """ Retrieves a single document/record. Consumed when a request hits an
        item endpoint (`/people/id/`).

        :param resource: resource being accessed. You should then use the
                         ``datasource`` helper function to retrieve both the
                         db collection/table and base query (filter), if any.
        :param req: an instance of ``eve.utils.ParsedRequest``. This contains
                    all the constraints that must be fulfilled in order to
                    satisfy the original request (where and sort parts, paging,
                    etc). As we are going to only look for one document here,
                    the only req attribute that you want to process here is
                    ``req.projection``.
        :param check_auth_value: a boolean flag indicating if the find
                                 operation should consider user-restricted
                                 resource access. Defaults to ``True``.
        :param force_auth_field_projection: a boolean flag indicating if the
                                            find operation should always
                                            include the user-restricted
                                            resource access field (if
                                            configured). Defaults to ``False``.

        :param lookup: the lookup fields. This will most likely be a record
                         id or, if alternate lookup is supported by the API,
                         the corresponding query.
        """

        data_source, filter_, sort, projection = self.datasource(resource)

        try:
            table = self.driver.Table(data_source)
            result = table.get_item(Key=lookup)
            return result['Item'] if 'Item' in result else None
        except BotoCoreClientError as e:
            abort(400, description=debug_error_message(e.response['Error']['Message']))

    def find_one_raw(self, resource, **lookup):
        """ Retrieves a single, raw document. No projections or datasource
        filters are being applied here. Just looking up the document using the
        same lookup.

        :param resource: resource name.
        :param ** lookup: lookup query.
        """

        id_field = config.DOMAIN[resource]["id_field"]
        _id = lookup.get(id_field)
        data_source, filter_, _, _ = self._datasource_ex(resource, {id_field: _id}, None)

        try:
            table = self.driver.Table(data_source)
            result = table.get_item(Key=lookup)
            return result['Item'] if 'Item' in result else None
        except BotoCoreClientError as e:
            abort(400, description=debug_error_message(e.response['Error']['Message']))

    def find_list_of_ids(self, resource, ids, client_projection=None):
        """ Retrieves a list of documents based on a list of primary keys
        The primary key is the field defined in `ID_FIELD`.
        This is a separate function to allow us to use per-database
        optimizations for this type of query.

        :param resource: resource name.
        :param ids: a list of ids corresponding to the documents
        to retrieve
        :param client_projection: a specific projection to use
        :return: a list of documents matching the ids in `ids` from the
        collection specified in `resource`
        """

        raise NotImplementedError

    def insert(self, resource, doc_or_docs):
        """ Inserts a document into a resource collection/table.

        :param resource: resource being accessed. You should then use
                         the ``datasource`` helper function to retrieve both
                         the actual datasource name.
        :param doc_or_docs: json document or list of json documents to be added
                            to the database.
        """

        raise NotImplementedError

    def update(self, resource, id_, updates, original):
        """ Updates a collection/table document/row.
        :param resource: resource being accessed. You should then use
                         the ``datasource`` helper function to retrieve
                         the actual datasource name.
        :param id_: the unique id of the document.
        :param updates: json updates to be performed on the database document
                        (or row).
        :param original: definition of the json document that should be
        updated.
        :raise OriginalChangedError: raised if the database layer notices a
        change from the supplied `original` parameter.
        """

        raise NotImplementedError

    def replace(self, resource, id_, document, original):
        """ Replaces a collection/table document/row.
        :param resource: resource being accessed. You should then use
                         the ``datasource`` helper function to retrieve
                         the actual datasource name.
        :param id_: the unique id of the document.
        :param document: the new json document
        :param original: definition of the json document that should be
        updated.
        :raise OriginalChangedError: raised if the database layer notices a
        change from the supplied `original` parameter.
        """

        raise NotImplementedError

    def remove(self, resource, lookup):
        """ Removes a document/row or an entire set of documents/rows from a
        database collection/table.

        :param resource: resource being accessed. You should then use
                         the ``datasource`` helper function to retrieve
                         the actual datasource name.
        :param lookup: a dict with the query that documents must match in order
                       to qualify for deletion. For single document deletes,
                       this is usually the unique id of the document to be
                       removed.
        """

        data_source, filter_, _, _ = self._datasource_ex(resource, lookup)

        # table = self.driver.Table(data_source)
        # result = table.get_item(Key=lookup)

    def combine_queries(self, query_a, query_b):
        """ Takes two db queries and applies db-specific syntax to produce the intersection.
        """

        raise NotImplementedError

    def get_value_from_query(self, query, field_name):
        """ Parses the given potentially-complex query and returns the value
        being assigned to the field given in `field_name`.

        This mainly exists to deal with more complicated compound queries
        """

        raise NotImplementedError

    def query_contains_field(self, query, field_name):
        """For the specified field name, does the query contain it?

        :param dict query: Filter query
        :param str field_name: Field name
        :return:
        """

        try:
            self.get_value_from_query(query, field_name)

        except KeyError:
            return False

        return True

    def is_empty(self, resource):
        """Returns True if the collection is empty. False otherwise

        :param str resource: Resource being accessed
        :return:
        """

        data_source, filter_, _, _ = self.datasource(resource)

        try:
            table = self.driver.Table(data_source)

            if not filter_:
                return table.scan()['Count'] == 0
            else:

                if config.LAST_UPDATED in filter_:
                    del filter_[config.LAST_UPDATED]

                # TODO: find() with filter
                # return coll.count_documents(filter_) == 0

                raise NotImplementedError

        except BotoCoreClientError as e:
            abort(400, description=debug_error_message(e.response['Error']['Message']))
