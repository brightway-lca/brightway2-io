# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.errors import StrategyError
from bw2io.strategies import link_iterable_by_fields
import copy
import unittest


class LinkIterableTestCase(unittest.TestCase):
    def test_all_datasets_in_target_have_database_field(self):
        self.assertEqual(
            link_iterable_by_fields(
                [],
                [{'database': 'foo', 'code': 'bar'}]
            ),
            []
        )
        with self.assertRaises(StrategyError):
            link_iterable_by_fields(
                [],
                [{'code': 'bar'}]
            )

    def test_all_datasets_in_target_have_code_field(self):
        self.assertEqual(
            link_iterable_by_fields(
                [],
                [{'database': 'foo', 'code': 'bar'}]
            ),
            []
        )
        with self.assertRaises(StrategyError):
            link_iterable_by_fields(
                [],
                [{'database': 'foo'}]
            )

    def test_nonunique_target_but_not_linked_no_error(self):
        data = [
            {'name': 'foo', 'database': 'a', 'code': 'b'},
            {'name': 'foo', 'database': 'a', 'code': 'c'},
            {'name': 'bar', 'database': 'a', 'code': 'd'}
        ]
        self.assertEqual(
            link_iterable_by_fields(
                [{'exchanges': [{'name': 'bar'}]}],
                data
            ),
            [{'exchanges': [{'name': 'bar', 'input': ('a', 'd')}]}],
        )

    def test_nonunique_target_raises_error(self):
        data = [
            {'name': 'foo', 'database': 'a', 'code': 'b'},
            {'name': 'foo', 'database': 'a', 'code': 'c'},
            {'name': 'bar', 'database': 'a', 'code': 'd'}
        ]
        with self.assertRaises(StrategyError):
            link_iterable_by_fields(
                [{'exchanges': [{'name': 'foo'}]}],
                data
            )

    def test_generic_linking_no_kind_no_relink(self):
        unlinked = [{
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
            }, {
                'name': 'foo',
                'unit': 'kilogram',
                'type': 'b',
            }]
        }]
        other = [{
            'name': 'foo',
            'categories': ('bar',),
            'database': 'db',
            'code': 'first'
        }, {
            'name': 'baz',
            'categories': ('bar',),
            'database': 'db',
            'code': 'second'
        }]
        expected = [{
            'exchanges': [{
                'name': 'foo',
                'type': 'a',
                'categories': ('bar',),
                'input': ('db', 'first')
            }, {
                'name': 'foo',
                'type': 'b',
                'unit': 'kilogram'
            }]
        }]
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, other)
        )

    def test_internal_linking(self):
        unlinked = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',)
            }, {
                'name': 'foo',
                'categories': ('baz',)
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        expected = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'input': ('db', 'first'),
            }, {
                'name': 'foo',
                'categories': ('baz',),
                'input': ('db', 'second'),
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, internal=True)
        )

    def test_kind_filter(self):
        unlinked = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
            }, {
                'name': 'foo',
                'categories': ('baz',)
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        expected = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
                'input': ('db', 'first'),
            }, {
                'name': 'foo',
                'categories': ('baz',),
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, internal=True, kind='a')
        )
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, internal=True, kind=['a'])
        )

    def test_kind_filter_and_relink(self):
        unlinked = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
                'input': ('something', 'else'),
            }, {
                'name': 'foo',
                'categories': ('baz',)
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        expected = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
                'input': ('db', 'first'),
            }, {
                'name': 'foo',
                'categories': ('baz',),
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, internal=True, kind='a', relink=True)
        )

    def test_relink(self):
        unlinked = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
                'input': ('something', 'else'),
            }, {
                'name': 'foo',
                'type': 'b',
                'input': ('something', 'else'),
                'categories': ('baz',)
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        expected = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
                'input': ('db', 'first'),
            }, {
                'name': 'foo',
                'type': 'b',
                'input': ('db', 'second'),
                'categories': ('baz',),
            }]
        }, {
            'database': 'db',
            'code': 'second',
            'name': 'foo',
            'categories': ('baz',),
            'exchanges': []
        }]
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, internal=True, relink=True)
        )

    def test_linking_with_fields(self):
        unlinked = [{
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'type': 'a',
            }, {
                'name': 'foo',
                'categories': ('baz',),
                'unit': 'kilogram',
                'type': 'b',
            }]
        }]
        other = [{
            'name': 'foo',
            'categories': ('bar',),
            'database': 'db',
            'code': 'first'
        }, {
            'name': 'foo',
            'categories': ('baz',),
            'database': 'db',
            'code': 'second'
        }]
        expected = [{
            'exchanges': [{
                'name': 'foo',
                'type': 'a',
                'categories': ('bar',),
                'input': ('db', 'first')
            }, {
                'name': 'foo',
                'type': 'b',
                'categories': ('baz',),
                'input': ('db', 'second'),
                'unit': 'kilogram'
            }]
        }]
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, other,
                                    fields=['name', 'categories'])
        )

    def test_no_relink_skips_linking(self):
        unlinked = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'input': ('something', 'else'),
            }]
        }]
        expected = [{
            'database': 'db',
            'code': 'first',
            'name': 'foo',
            'categories': ('bar',),
            'exchanges': [{
                'name': 'foo',
                'categories': ('bar',),
                'input': ('db', 'first'),
            }]
        }]
        self.assertEqual(
            unlinked,
            link_iterable_by_fields(copy.deepcopy(unlinked), internal=True)
        )
        del unlinked[0]['exchanges'][0]['input']
        self.assertEqual(
            expected,
            link_iterable_by_fields(unlinked, internal=True)
        )
