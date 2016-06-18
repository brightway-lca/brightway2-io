# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.strategies import (
    add_activity_hash_code,
    drop_unlinked_cfs,
    match_subcategories,
    set_biosphere_type,
)
from bw2data import Database
from bw2data.tests import BW2DataTest
import unittest


class LCIATestCase(unittest.TestCase):
    def test_add_activity_hash_code(self):
        data = [{
            'exchanges': [{
                'name': 'foo',
                'code': 'bar'
            }, {
                'name': 'foo',
            }]
        }]
        expected = [{
            'exchanges': [{
                'name': 'foo',
                'code': 'bar'
            }, {
                'name': 'foo',
                'code': 'acbd18db4cc2f85cedef654fccc4a4d8',
            }]
        }]
        self.assertEqual(
            expected,
            add_activity_hash_code(data)
        )

    def test_drop_unlinked_cfs(self):
        data = [{
            'exchanges': [{
                'name': 'Boron trifluoride',
                'input': 'something',
                'categories': ('air',),
                'unit': 'kilogram',
                'amount': 1,
            }, {
                'name': 'Boron trifluoride',
                'categories': ('air', 'another'),
                'unit': 'kilogram',
                'type': 'biosphere',
                'amount': 1,
            }]
        }]
        expected = [{
            'exchanges': [{
                'name': 'Boron trifluoride',
                'input': 'something',
                'categories': ('air',),
                'unit': 'kilogram',
                'amount': 1,
            }]
        }]
        self.assertEqual(
            expected,
            drop_unlinked_cfs(data)
        )

    def test_set_biosphere_type(self):
        data = [{
            'exchanges': [{}, {}]
        }]
        expected = [{
            'exchanges': [
                {'type': 'biosphere'},
                {'type': 'biosphere'},
            ]
        }]
        self.assertEqual(
            expected,
            set_biosphere_type(data)
        )


class LCIATestCase2(BW2DataTest):
    def test_match_subcategories(self):
        self.maxDiff = None
        background = [
            {
                'categories': ('air', 'non-urban air or from high stacks'),
                'code': 'first',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {
                'categories': ('air', 'low population density, long-term'),
                'code': 'second',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {
                'categories': ('air', 'lower stratosphere + upper troposphere'),
                'code': 'third',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {  # Skip - root category
                'categories': ('air',),
                'code': 'fourth',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {  # Should be skipped - wrong type
                'categories': ('air', 'skip me'),
                'code': 'Bill. My friends just call me Bill.',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'something else',
                'unit': 'kilogram'
            }
        ]
        db = Database('b')
        db.register()
        db.write({(obj['database'], obj['code']): obj
                  for obj in background})

        data = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                # Only for CFs - no need for biosphere filter
                # 'type': 'biosphere',
                'amount': 1,
            }, {
                'name': 'Boron trifluoride',
                'categories': ('air', 'lower stratosphere + upper troposphere'),
                'unit': 'kilogram',
                'amount': 0,
            }]
        }]
        expected = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                'amount': 1,
            }, {  # Not linked - already has subcategories
                'categories': ('air',
                               'lower stratosphere + upper troposphere'),
                'name': 'Boron trifluoride',
                'unit': 'kilogram',
                'amount': 0,
            }, {
                'categories': ('air',
                               'low population density, long-term'),
                'database': 'b',
                'name': 'Boron trifluoride',
                'unit': 'kilogram',
                'input': ('b', 'second'),
                'amount': 1,
            }, {
                'amount': 1,
                'categories': ('air',
                               'non-urban air or from high stacks'),
                'database': 'b',
                'input': ('b', 'first'),
                'name': 'Boron trifluoride',
                'unit': 'kilogram'
            }]
        }]
        answer = match_subcategories(data, 'b', remove=False)
        self.assertEqual(
            expected,
            answer
        )

    def test_match_subcategories_remove(self):
        self.maxDiff = None
        background = [
            {
                'categories': ('air', 'non-urban air or from high stacks'),
                'code': 'first',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {
                'categories': ('air', 'low population density, long-term'),
                'code': 'second',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {
                'categories': ('air', 'lower stratosphere + upper troposphere'),
                'code': 'third',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {  # Skip - root category
                'categories': ('air',),
                'code': 'fourth',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {  # Should be skipped - wrong type
                'categories': ('air', 'skip me'),
                'code': 'Bill. My friends just call me Bill.',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'something else',
                'unit': 'kilogram'
            }
        ]
        db = Database('b')
        db.register()
        db.write({(obj['database'], obj['code']): obj
                  for obj in background})

        data = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                # Only for CFs - no need for biosphere filter
                # 'type': 'biosphere',
                'amount': 1,
            }]
        }]
        expected = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'categories': ('air',
                               'low population density, long-term'),
                'database': 'b',
                'name': 'Boron trifluoride',
                'unit': 'kilogram',
                'input': ('b', 'second'),
                'amount': 1,
            }, {
                'categories': ('air',
                               'lower stratosphere + upper troposphere'),
                'database': 'b',
                'name': 'Boron trifluoride',
                'unit': 'kilogram',
                'input': ('b', 'third'),
                'amount': 1,
            }, {
                'amount': 1,
                'categories': ('air',
                               'non-urban air or from high stacks'),
                'database': 'b',
                'input': ('b', 'first'),
                'name': 'Boron trifluoride',
                'unit': 'kilogram'
            }]
        }]
        self.assertEqual(
            expected,
            match_subcategories(data, 'b')
        )

    def test_match_subcategories_not_remove(self):
        self.maxDiff = None
        background = [
            {
                'categories': ('air', 'non-urban air or from high stacks'),
                'code': 'first',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {
                'categories': ('air', 'low population density, long-term'),
                'code': 'second',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {
                'categories': ('air', 'lower stratosphere + upper troposphere'),
                'code': 'third',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {  # Skip - root category
                'categories': ('air',),
                'code': 'fourth',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
            }, {  # Should be skipped - wrong type
                'categories': ('air', 'skip me'),
                'code': 'Bill. My friends just call me Bill.',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'something else',
                'unit': 'kilogram'
            }
        ]
        db = Database('b')
        db.register()
        db.write({(obj['database'], obj['code']): obj
                  for obj in background})

        data = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                # Only for CFs - no need for biosphere filter
                # 'type': 'biosphere',
                'amount': 1,
                'input': ('foo', 'bar'),
            }]
        }]
        expected = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                'amount': 1,
                'input': ('foo', 'bar'),
            }, {
                'categories': ('air',
                               'low population density, long-term'),
                'database': 'b',
                'name': 'Boron trifluoride',
                'unit': 'kilogram',
                'input': ('b', 'second'),
                'amount': 1,
            }, {
                'categories': ('air',
                               'lower stratosphere + upper troposphere'),
                'database': 'b',
                'name': 'Boron trifluoride',
                'unit': 'kilogram',
                'input': ('b', 'third'),
                'amount': 1,
            }, {
                'amount': 1,
                'categories': ('air',
                               'non-urban air or from high stacks'),
                'database': 'b',
                'input': ('b', 'first'),
                'name': 'Boron trifluoride',
                'unit': 'kilogram'
            }]
        }]
        self.assertEqual(
            expected,
            match_subcategories(data, 'b')
        )

    def test_match_subcategories_makes_copies(self):
        """Should copy data instead of creating references, so that there are different amounts for different methods."""
        self.maxDiff = None
        background = [{
                'categories': ('air', 'non-urban air or from high stacks'),
                'code': 'first',
                'database': 'b',
                'exchanges': [],
                'name': 'Boron trifluoride',
                'type': 'emission',
                'unit': 'kilogram'
        }]
        db = Database('b')
        db.register()
        db.write({(obj['database'], obj['code']): obj
                  for obj in background})
        data = [{
            'name': 'Some LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                'amount': 1,
                'input': ('foo', 'bar'),
            }]
        }, {
            'name': 'Another LCIA method',
            'exchanges': [{
                'name': 'Boron trifluoride',
                'categories': ('air',),
                'unit': 'kilogram',
                'amount': 2,
                'input': ('foo', 'bar'),
            }]
        }]
        result = match_subcategories(data, 'b')
        for cf in result[0]['exchanges']:
            self.assertEqual(cf['amount'], 1)
        for cf in result[1]['exchanges']:
            self.assertEqual(cf['amount'], 2)
