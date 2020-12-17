"""test_expression

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

import pytest
from boto3.dynamodb.conditions import Attr, Key
from eve_dynamodb.expression import build_attr_expression, build_key_expression


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
def test_single_attr_expression(query: dict, expectation: Attr):
    """Test to ensure attr expressions are properly built from queries

    :param dict query: Query to build expression from
    :param Attr expectation: Expected expression
    :raises: AssertionError
    """

    assert build_attr_expression(query) == expectation


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
def test_attr_compound_expression(query: dict, expectation: Attr):
    """Test to ensure attr compound expressions are properly built from queries

    :param dict query: Query to build expression from
    :param Attr expectation: Expected expression
    :raises: AssertionError
    """

    assert build_attr_expression(query) == expectation


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'foo': {'$lt': 1, '$exists': True}}, Attr('foo').lt(1) & Attr('foo').exists()),
        (
                {'foo': {'$contains': 'bar', '$between': ['a', 'c']}},
                Attr('foo').contains('bar') & Attr('foo').between('a', 'c')
        )
))
def test_attr_expression_with_multiple_conditions(query: dict, expectation: Attr):
    """Test to ensure attr expressions with multiple conditions are properly built from queries

    :param dict query: Query to build expression from
    :param Attr expectation: Expected expression
    :raises: AssertionError
    """

    assert build_attr_expression(query) == expectation


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
def test_attr_expression_logical_operators(query: dict, expectation: Attr):
    """Test to ensure attr expressions can be built with logical operators

    :param dict query: Query to build expression from
    :param Attr expectation: Expected expression
    :raises: AssertionError
    """

    assert build_attr_expression(query) == expectation


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'foo': 'bar'}, Key('foo').eq('bar')),
        ({'foo': {'$eq': 'bar'}}, Key('foo').eq('bar')),
        ({'foo': {'$lt': 1}}, Key('foo').lt(1)),
        ({'foo': {'$lte': 2}}, Key('foo').lte(2)),
        ({'foo': {'$gt': 3}}, Key('foo').gt(3)),
        ({'foo': {'$gte': 4}}, Key('foo').gte(4)),
        ({'foo': {'$between': [1, 3]}}, Key('foo').between(1, 3)),
        ({'foo': {'$startsWith': 'bar'}}, Key('foo').begins_with('bar'))
))
def test_single_key_expression(query: dict, expectation: Key):
    """Test to ensure key expressions are properly built from queries

    :param dict query: Query to build expression from
    :param Key expectation: Expected expression
    :raises: AssertionError
    """

    assert build_key_expression(query) == expectation


@pytest.mark.parametrize(('query', 'expectation'), (
        ({'foo': 'bar', 'baz': 'hello'}, Key('foo').eq('bar') & Key('baz').eq('hello')),
        ({'foo': {'$eq': 'bar'}, 'baz': {'$eq': 'world'}}, Key('foo').eq('bar') & Key('baz').eq('world')),
        ({'foo': {'$lt': 1}, 'baz': {'$lt': 2}}, Key('foo').lt(1) & Key('baz').lt(2)),
        ({'foo': {'$lte': 2}, 'baz': {'$lte': 3}}, Key('foo').lte(2) & Key('baz').lte(3)),
        ({'foo': {'$gt': 3}, 'baz': {'$gt': 4}}, Key('foo').gt(3) & Key('baz').gt(4)),
        ({'foo': {'$gte': 4}, 'baz': {'$gte': 5}}, Key('foo').gte(4) & Key('baz').gte(5)),
        ({'x': {'$between': [1, 3], 'y': {'$between': [4, 5]}}}, Key('x').between(1, 3) & Key('y').between(4, 5)),
        ({'x': {'$startsWith': 'a'}, 'y': {'$startsWith': 'b'}}, Key('x').begins_with('a') & Key('y').begins_with('b'))
))
def test_compound_expression(query: dict, expectation: Key):
    """Test to ensure compound key expressions are properly built from queries

    :param dict query: Query to build expression from
    :param Key expectation: Expected expression
    :raises: AssertionError
    """

    assert build_key_expression(query) == expectation
