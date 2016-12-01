# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.errors import StrategyError
from bw2io.strategies import (
    assign_only_product_as_production,
    convert_uncertainty_types_to_integers,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    link_technosphere_by_activity_hash,
    set_code_by_activity_hash,
    tupleize_categories,
)
from bw2data import Database
import copy
import numpy as np
import unittest


class GenericStrategiesTestCase(unittest.TestCase):
    def test_tupleize_exchanges(self):
        ds = [{
            'exchanges': [{
                'categories': ['resource', 'in ground'],
            }]
        }]
        expected = [{
            'exchanges': [{
                'categories': (u'resource', u'in ground'),
            }]
        }]
        self.assertEqual(
            expected,
            tupleize_categories(ds)
        )

    def test_tupleize_datasets(self):
        ds = [{
            'categories': ['resource', 'in ground'],
        }]
        expected = [{
            'categories': (u'resource', u'in ground'),
        }]
        self.assertEqual(
            expected,
            tupleize_categories(ds)
        )

    def test_assign_only_product_as_production(self):
        ds = [{'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }]}]
        expected = [{
            'exchanges': [{
                'amount': 2,
                'unit': 'kilogram',
                'name': 'foo',
                'type': 'production',
            }],
            'name': 'foo',
            'unit': 'kilogram',
            'production amount': 2,
        }]
        self.assertEqual(
            assign_only_product_as_production(ds),
            expected
        )

    def test_assign_only_product_already_reference_product(self):
        ds = [{
            'exchanges': [{
                'amount': 2,
                'unit': 'kilogram',
                'name': 'foo',
                'type': 'production',
            }],
            'reference product': 'foo',
        }]
        self.assertEqual(
            ds,
            assign_only_product_as_production(copy.deepcopy(ds)),
        )

    def test_assign_only_product_no_products(self):
        ds = [{'name': 'hi'}]
        self.assertEqual(
            ds,
            assign_only_product_as_production(copy.deepcopy(ds)),
        )

    def test_assign_only_product_multiple_products(self):
        ds = [{'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }, {
            'amount': 3,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }]}]
        self.assertEqual(
            ds,
            assign_only_product_as_production(copy.deepcopy(ds)),
        )

    def test_set_code_by_activity_hash(self):
        ds = [{'name': 'hi'}]
        expected = [{
            'code': '49f68a5c8493ec2c0bf489821c21fc3b',
            'name': 'hi',
        }]
        self.assertEqual(
            set_code_by_activity_hash(ds),
            expected
        )

    def test_set_code_by_activity_hash_overwrite(self):
        ds = [{
            'code': 'foo',
            'name': 'hi'
        }]
        expected = [{
            'code': '49f68a5c8493ec2c0bf489821c21fc3b',
            'name': 'hi',
        }]
        assert set_code_by_activity_hash(ds)[0]['code'] == 'foo'
        assert set_code_by_activity_hash(ds, True) == expected

    # def test_link_technosphere_internal(self):
    #     unlinked = [{
    #         'exchanges': [{
    #             'name': 'foo',
    #             'type': 'technosphere',
    #         }]
    #     }]
    #     other = [{
    #         'name': 'foo',
    #         'type': 'process',
    #         'database': 'db',
    #         'code': 'first'
    #     }]
    #     expected = [{
    #         'exchanges': [{
    #             'name': 'foo',
    #             'type': 'technosphere',
    #             'input': ('db', 'first')
    #         }]
    #     }]
    #     self.assertEqual(
    #         expected,
    #         link_technosphere_by_activity_hash(unlinked, other)
    #     )

    def test_link_technosphere_wrong_type(self):
        pass

    def test_link_technosphere_fields(self):
        pass

    def test_link_technosphere_external(self):
        pass

    def test_link_technosphere_external_untyped(self):
        pass




def test_convert_uncertainty_types_to_integers():
    data = [{'exchanges': [
        {'uncertainty type': 1.2},
        {'uncertainty type': False},
        {'uncertainty type': 42},
    ]}]
    expected = [{'exchanges': [
        {'uncertainty type': 1},
        {'uncertainty type': False},
        {'uncertainty type': 42},
    ]}]
    result = convert_uncertainty_types_to_integers(data)
    assert result == expected


def test_drop_falsey_uncertainty_fields_but_keep_zeros():
    data = [{'exchanges': [
        {'loc': None},
        {'shape': ()},
        {'scale': ''},
        {'minimum': []},
        {'maximum': {}},
    ]}]
    expected = [{'exchanges': [{}, {}, {}, {}, {}]}]
    result = drop_falsey_uncertainty_fields_but_keep_zeros(data)
    assert result == expected

    data = [{'exchanges': [
        {'loc': "  "},
        {'shape': True},
        {'scale': 0},
        {'minimum': 0.0},
        {'maximum': 42},
    ]}]
    expected = [{'exchanges': [
        {'loc': "  "},
        {'shape': True},
        {'scale': 0},
        {'minimum': 0.0},
        {'maximum': 42},
    ]}]
    result = drop_falsey_uncertainty_fields_but_keep_zeros(data)
    assert result == expected

    data = [{'exchanges': [{'loc': np.nan}]}]
    result = drop_falsey_uncertainty_fields_but_keep_zeros(data)
    assert len(result) == 1 and len(result[0]['exchanges']) == 1
    assert np.isnan(result[0]['exchanges'][0]['loc'])

    data = [{'exchanges': [{'loc': False}]}]
    expected = [{'exchanges': [{'loc': False}]}]
    result = drop_falsey_uncertainty_fields_but_keep_zeros(data)
    assert result == expected
