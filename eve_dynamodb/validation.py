"""
"""

from eve.io.mongo.validation import Validator


class ValidatorDynamoDB(Validator):
    """Eve Mongo Validator subclass adding support for DynamoDB
    """

    def _is_value_unique(self, unique, field, value, query):
        """ Validates that a field value is unique
        """

        """
        if unique:
            query[field] = value

            resource_config = config.DOMAIN[self.resource]

            label, _, _, _ = app.data.datasource(self.resource)
            selected = app.data.driver.select(label, **query)

            # exclude current document
            if self._id:
                id_field = resource_config['id_field']
                selected = selected.where(
                    '_.%s <> "%s"' % (id_field, self._id))

            if count_selection(selected) > 0:
                self._error(field, "value '%s' is not unique" % value)
        """

        raise NotImplementedError

    # Override validation for Mongo fields
    def _validate_type_objectid(self, field, value):
        """Validates that a field is a valid objectid

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field objectid is not valid on DynamoDB.")

    def _validate_type_dbref(self, field, value):
        """Validates that a field is a valid database reference

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field dbref is not valid on DynamoDB.")

    def _validate_type_point(self, field, value):
        """Validates that a field is a valid point

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field point is not valid on DynamoDB.")

    def _validate_type_geometrycollection(self, field, value):
        """Validates that a field is a valid geometry collection

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field geometrycollection is not valid on DynamoDB.")

    def _validate_type_multipolygon(self, field, value):
        """Validates that a field is a valid multi-polygon

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field multipolygon is not valid on DynamoDB.")

    def _validate_type_multilinestring(self, field, value):
        """Validates that a field is a valid multi-line string

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field multilinestring is not valid on DynamoDB.")

    def _validate_type_multipoint(self, field, value):
        """Validates that a field is a valid multi point

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field multipoint is not valid on DynamoDB.")

    def _validate_type_polygon(self, field, value):
        """Validates that a field is a valid polygon

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field polygon is not valid on DynamoDB.")

    def _validate_type_linestring(self, field, value):
        """Validates that a field is a valid line string

        :param field:
        :param value:
        :return:
        """

        self._error(field, "field linestring is not valid on DynamoDB.")
