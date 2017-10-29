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
from copy import deepcopy
import numpy as np
import pytest


def test_tupleize_exchanges():
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
    assert expected == tupleize_categories(ds)

def test_tupleize_datasets():
    ds = [{
        'categories': ['resource', 'in ground'],
    }]
    expected = [{
        'categories': (u'resource', u'in ground'),
    }]
    assert expected == tupleize_categories(ds)

def test_assign_only_product_as_production():
    ds = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }]
    }]
    expected = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }],
        'name': 'foo',
        'reference product': 'foo',
        'unit': 'kilogram',
        'production amount': 2,
    }]
    assert assign_only_product_as_production(ds) == expected

def test_assign_only_product_no_name():
    ds = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': '',
            'type': 'production',
        }]
    }]
    with pytest.raises(AssertionError):
        assign_only_product_as_production(ds)
    ds = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'type': 'production',
        }]
    }]
    with pytest.raises(KeyError):
        assign_only_product_as_production(ds)

def test_assign_only_product_leave_fields():
    ds = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }],
        'name': 'bar',
        'unit': 'bar',
        'production amount': 12,
    }]
    expected = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }],
        'name': 'bar',
        'reference product': 'foo',
        'unit': 'bar',
        'production amount': 2,
    }]
    assert assign_only_product_as_production(ds) == expected

def test_assign_only_product_already_reference_product():
    ds = [{
        'exchanges': [{
            'amount': 2,
            'unit': 'kilogram',
            'name': 'foo',
            'type': 'production',
        }],
        'reference product': 'bar',
    }]
    assert ds == assign_only_product_as_production(deepcopy(ds))

def test_assign_only_product_no_products():
    ds = [{'name': 'hi'}]
    assert ds == assign_only_product_as_production(deepcopy(ds))

def test_assign_only_product_multiple_products():
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
    assert ds == assign_only_product_as_production(deepcopy(ds))

def test_set_code_by_activity_hash():
    ds = [{'name': 'hi'}]
    expected = [{
        'code': '49f68a5c8493ec2c0bf489821c21fc3b',
        'name': 'hi',
    }]
    assert set_code_by_activity_hash(ds) == expected

def test_set_code_by_activity_hash_overwrite():
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

# def test_link_technosphere_internal():
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

def test_link_technosphere_wrong_type():
    pass

def test_link_technosphere_fields():
    pass

def test_link_technosphere_external():
    pass

def test_link_technosphere_external_untyped():
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
