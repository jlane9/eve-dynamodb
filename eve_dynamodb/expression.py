"""Build DynamoDB query expressions

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

from functools import reduce
from boto3.dynamodb.conditions import Attr, ConditionBase, Key


def and_attr_conditions(acc: Attr, val: dict) -> ConditionBase:
    """Logically 'AND' conditions together

    :param Attr acc: Condition expression
    :param dict val: Expression to combine
    :return: Combined expression
    :rtype: ConditionBase
    """

    return (acc & build_attr_expression(val)) if isinstance(acc, ConditionBase) else build_attr_expression(val)


def or_attr_conditions(acc: Attr, val: dict) -> ConditionBase:
    """Logically 'OR' conditions together

    :param Attr acc: Condition expression
    :param dict val: Expression to combine
    :return: Combined expression
    :rtype: ConditionBase
    """

    return (acc | build_attr_expression(val)) if isinstance(acc, ConditionBase) else build_attr_expression(val)


def build_attr_expression(lookup: dict, parent: str = None) -> ConditionBase:
    """Build an attr expression from a query

    :param dict lookup: Query expression
    :param str parent: Parent key
    :return: Combined expression
    :rtype: ConditionBase
    """

    operators = {
        '$eq': lambda key, value: Attr(key).eq(value),
        '$ne': lambda key, value: Attr(key).ne(value),
        '$lt': lambda key, value: Attr(key).lt(value),
        '$lte': lambda key, value: Attr(key).lte(value),
        '$gt': lambda key, value: Attr(key).gt(value),
        '$gte': lambda key, value: Attr(key).gte(value),
        '$in': lambda key, value: Attr(key).is_in(value),
        '$nin': lambda key, value: ~Attr(key).is_in(value),
        '$between': lambda key, value: Attr(key).between(*value),
        '$contains': lambda key, value: Attr(key).contains(value),
        '$exists': lambda key, value: Attr(key).exists() if value else Attr(key).not_exists(),
        '$size': lambda key, value: Attr(key).size().eq(value),
        '$startsWith': lambda key, value: Attr(key).begins_with(value),
        '$type': lambda key, value: Attr(key).attribute_type(value)
    }

    logical_operators = ['$not', '$and', '$or', '$nor', '$xor']

    operations = []

    for k, v in lookup.items():
        operator = operators.get(k, operators['$eq'])

        if isinstance(v, dict):
            operations.append(build_attr_expression(v, k))

        elif k in logical_operators and isinstance(v, (list, tuple)):

            if k == '$not':
                operations.append(~reduce(and_attr_conditions, v, None))

            elif k == '$and':
                operations.append(reduce(and_attr_conditions, v, None))

            elif k == '$or':
                operations.append(reduce(or_attr_conditions, v, None))

            elif k == '$nor':
                operations.append(~reduce(or_attr_conditions, v, None))

            elif k == '$xor' and len(v) == 2:

                x, y = build_attr_expression(v[0]), build_attr_expression(v[1])
                operations.append((x & ~y) | (~x & y))

            else:
                raise ValueError("Error: $xor can only be computed against two values at a time")

        else:
            operations.append(operator(parent if parent else k, v))

    return reduce(lambda acc, val: (acc & val) if acc else val, operations)


def build_key_expression(lookup: dict, parent: str = None) -> ConditionBase:
    """Build a key expression from a query

    :param dict lookup: Query expression
    :param str parent: Parent key
    :return: Combined expression
    :rtype: ConditionBase
    """

    operators = {
        '$eq': lambda key, value: Key(key).eq(value),
        '$lt': lambda key, value: Key(key).lt(value),
        '$lte': lambda key, value: Key(key).lte(value),
        '$gt': lambda key, value: Key(key).gt(value),
        '$gte': lambda key, value: Key(key).gte(value),
        '$between': lambda key, value: Key(key).between(*value),
        '$startsWith': lambda key, value: Key(key).begins_with(value)
    }

    operations = []

    for k, v in lookup.items():
        operator = operators.get(k, operators['$eq'])

        if isinstance(v, dict):
            operations.append(build_key_expression(v, k))

        else:
            operations.append(operator(parent if parent else k, v))

    return reduce(lambda acc, val: (acc & val) if acc else val, operations)
