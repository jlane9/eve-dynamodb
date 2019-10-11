"""expression

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

from functools import reduce
from boto3.dynamodb.conditions import Attr, ConditionBase


def and_conditions(acc, val):
    return (acc & build_expression(val)) if isinstance(acc, ConditionBase) else build_expression(val)


def or_conditions(acc, val):
    return (acc | build_expression(val)) if isinstance(acc, ConditionBase) else build_expression(val)


def build_expression(lookup, parent=None):
    """Build an expression from a query

    :param dict lookup:
    :param str parent: Parent key
    :return:
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
        '$type': lambda key, value: Attr(key).attribute_type(value),
    }

    logical_operators = ['$not', '$and', '$or', '$nor', '$xor']

    operations = []

    for k, v in lookup.items():
        operator = operators.get(k, operators['$eq'])

        if isinstance(v, dict):
            operations.append(build_expression(v, k))

        elif k in logical_operators and isinstance(v, (list, tuple)):

            if k == '$not':
                operations.append(~reduce(and_conditions, v, None))

            elif k == '$and':
                operations.append(reduce(and_conditions, v, None))

            elif k == '$or':
                operations.append(reduce(or_conditions, v, None))

            elif k == '$nor':
                operations.append(~reduce(or_conditions, v, None))

            elif k == '$xor' and len(v) == 2:

                x, y = build_expression(v[0]), build_expression(v[1])
                operations.append((x & ~y) | (~x & y))

            else:
                raise ValueError("Error: $xor can only be computed against two values at a time")

        else:
            operations.append(operator(parent if parent else k, v))

    return reduce(lambda acc, val: (acc & val) if acc else val, operations)
