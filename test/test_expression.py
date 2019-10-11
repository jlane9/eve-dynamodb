"""test_expression

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

import pytest
from boto3.dynamodb.conditions import Attr
from eve_dynamodb.expression import build_expression


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'foo': 'bar'}, Attr('foo').eq('bar')),
        ({'foo': {'$eq': 'bar'}}, Attr('foo').eq('bar')),
        ({'foo': {'$ne': 'bar'}}, Attr('foo').ne('bar')),
        ({'foo': {'$lt': 1}}, Attr('foo').lt(1)),
        ({'foo': {'$lte': 2}}, Attr('foo').lte(2)),
        ({'foo': {'$gt': 3}}, Attr('foo').gt(3)),
        ({'foo': {'$gte': 4}}, Attr('foo').gte(4)),
        ({'foo': {'$in': [1, 2, 3]}}, Attr('foo').is_in([1, 2, 3])),
        ({'foo': {'$nin': [4, 5, 6]}}, ~Attr('foo').is_in([4, 5, 6])),
        ({'foo': {'$between': [1, 3]}}, Attr('foo').between(1, 3)),
        ({'foo': {'$contains': 'bar'}}, Attr('foo').contains('bar')),
        ({'foo': {'$exists': True}}, Attr('foo').exists()),
        ({'foo': {'$exists': False}}, Attr('foo').not_exists()),
        ({'foo': {'$size': 1}}, Attr('foo').size().eq(1)),
        ({'foo': {'$startsWith': 'bar'}}, Attr('foo').begins_with('bar')),
        ({'foo': {'$type': 'number'}}, Attr('foo').attribute_type('number'))
))
def test_single_expression(query, expectation):
    """Test to ensure expressions are properly built from queries

    :param dict query: Query to build expression from
    :param expectation: Expected expression
    :return:
    """

    assert build_expression(query) == expectation


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'foo': 'bar', 'baz': 'hello'}, Attr('foo').eq('bar') & Attr('baz').eq('hello')),
        ({'foo': {'$eq': 'bar'}, 'baz': {'$eq': 'world'}}, Attr('foo').eq('bar') & Attr('baz').eq('world')),
        ({'foo': {'$ne': 'bar'}, 'baz': {'ne': 'foobar'}}, Attr('foo').ne('bar') & Attr('baz').eq('foobar')),
        ({'foo': {'$lt': 1}, 'baz': {'$lt': 2}}, Attr('foo').lt(1) & Attr('baz').lt(2)),
        ({'foo': {'$lte': 2}, 'baz': {'$lte': 3}}, Attr('foo').lte(2) & Attr('baz').lte(3)),
        ({'foo': {'$gt': 3}, 'baz': {'$gt': 4}}, Attr('foo').gt(3) & Attr('baz').gt(4)),
        ({'foo': {'$gte': 4}, 'baz': {'$gte': 5}}, Attr('foo').gte(4) & Attr('baz').gte(5)),
        ({'foo': {'$in': [1, 2]}, 'baz': {'$in': [4, 5]}}, Attr('foo').is_in([1, 2]) & Attr('baz').is_in([4, 5])),
        ({'x': {'$nin': [3, 4]}, 'y': {'$nin': [4, 5]}},  ~Attr('x').is_in([3, 4]) & ~Attr('y').is_in([4, 5])),
        ({'x': {'$between': [1, 3], 'y': {'$between': [4, 5]}}}, Attr('x').between(1, 3) & Attr('y').between(4, 5)),
        ({'x': {'$contains': 'ab'}, 'y': {'$contains': 'bc'}}, Attr('x').contains('ab') & Attr('y').contains('bc')),
        ({'foo': {'$exists': True}, 'baz': {'$exists': True}}, Attr('foo').exists() & Attr('baz').exists()),
        ({'foo': {'$exists': False}, 'baz': {'$exists': False}}, Attr('foo').not_exists() & Attr('baz').not_exists()),
        ({'foo': {'$size': 1}, 'baz': {'$size': 2}}, Attr('foo').size().eq(1) & Attr('baz').size().eq(2)),
        (
                {'x': {'$startsWith': 'a'}, 'y': {'$startsWith': 'b'}},
                Attr('x').begins_with('a') & Attr('y').begins_with('b')
        ),
        (
                {'x': {'$type': 'int'}, 'y': {'$type': 'str'}},
                Attr('x').attribute_type('int') & Attr('y').attribute_type('str')
        )
))
def test_compound_expression(query, expectation):
    """Test to ensure compound expressions are properly built from queries

    :param dict query: Query to build expression from
    :param expectation: Expected expression
    :return:
    """

    assert build_expression(query) == expectation


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'foo': {'$lt': 1, '$exists': True}}, Attr('foo').lt(1) & Attr('foo').exists()),
        (
                {'foo': {'$contains': 'bar', '$between': ['a', 'c']}},
                Attr('foo').contains('bar') & Attr('foo').between('a', 'c')
        )
))
def test_expression_with_multiple_conditions(query, expectation):
    """Test to ensure expressions with multiple conditions are properly built from queries

    :param dict query: Query to build expression from
    :param expectation: Expected expression
    :return:
    """

    assert build_expression(query) == expectation


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'$not': [{'a': 'foo'}, {'b': 'bar'}]}, ~(Attr('a').eq('foo') & Attr('b').eq('bar'))),
        ({'$and': [{'foo': 'bar'}, {'baz': 'hello'}]}, Attr('foo').eq('bar') & Attr('baz').eq('hello')),
        ({'$or': [{'x': 'foo'}, {'y': 'bar'}]}, (Attr('x').eq('foo') | Attr('y').eq('bar'))),
        ({'$nor': [{'foo': 'bar'}, {'baz': 'hello'}]}, ~(Attr('foo').eq('bar') | Attr('baz').eq('hello'))),
        (
                {'$xor': [{'x': 'foo'}, {'y': 'bar'}]},
                ((Attr('x').eq('foo') & ~Attr('y').eq('bar')) | (~Attr('x').eq('foo') & Attr('y').eq('bar')))
        )
))
def test_expression_logical_operators(query, expectation):
    """

    :param dict query: Query to build expression from
    :param expectation: Expected expression
    :return:
    """

    assert build_expression(query) == expectation
