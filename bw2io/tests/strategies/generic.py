from ...errors import StrategyError
from ...strategies import tupleize_categories
import copy
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
                'categories': (u'natural resource', u'in ground'),
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
            'categories': (u'natural resource', u'in ground'),
        }]
        self.assertEqual(
            expected,
            tupleize_categories(ds)
        )
