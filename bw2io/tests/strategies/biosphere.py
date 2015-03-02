from ...strategies import (
    link_biosphere_by_activity_hash,
    normalize_biosphere_categories,
    normalize_biosphere_names,
)
from ...errors import StrategyError
import copy
import unittest


class BiosphereNameNormalizationTestCase(unittest.TestCase):
    def test_normalize_ds_name(self):
        ds = [{
            'categories': ['air'],
            'name': "Carbon dioxide, biogenic",
            'type': "emission",
        }]
        expected = [{
            'categories': ['air'],
            'name': "Carbon dioxide, non-fossil",
            'type': "emission",
        }]
        self.assertEqual(
            expected,
            normalize_biosphere_names(ds)
        )

    def test_missing_ds_name_raises_error(self):
        ds = [{
            'categories': ['air'],
            'type': "emission",
        }]
        with self.assertRaises(StrategyError):
            normalize_biosphere_names(ds)

    def test_normalize_ds_name_not_emission(self):
        ds = [{
            'categories': ['air'],
            'name': "Carbon dioxide, biogenic",
        }]
        self.assertEqual(
            ds,
            normalize_biosphere_names(copy.deepcopy(ds))
        )

    def test_normalize_ds_name_no_category(self):
        ds = [{
            'name': "Carbon dioxide, biogenic",
            'type': "emission",
        }]
        self.assertEqual(
            ds,
            normalize_biosphere_names(copy.deepcopy(ds))
        )

    def test_normalize_exc_name(self):
        ds = [{
            'exchanges': [{
                'categories': ['air'],
                'name': "Carbon dioxide, biogenic",
                'type': "biosphere",
            }]
        }]
        expected = [{
            'exchanges': [{
                'categories': ['air'],
                'name': "Carbon dioxide, non-fossil",
                'type': "biosphere",
            }]
        }]
        self.assertEqual(
            expected,
            normalize_biosphere_names(ds)
        )

    def test_normalize_exc_name_already_linked(self):
        ds = [{
            'exchanges': [{
                'categories': ['air'],
                'name': "Carbon dioxide, biogenic",
                'type': "biosphere",
                'input': ('foo', 'bar'),
            }]
        }]
        expected = [{
            'exchanges': [{
                'categories': ['air'],
                'name': "Carbon dioxide, non-fossil",
                'type': "biosphere",
                'input': ('foo', 'bar'),
            }]
        }]
        self.assertEqual(
            expected,
            normalize_biosphere_names(ds)
        )

    def test_normalize_exc_name_not_biosphere(self):
        ds = [{
            'exchanges': [{
                'categories': ['air'],
                'name': "Carbon dioxide, biogenic",
            }]
        }]
        self.assertEqual(
            ds,
            normalize_biosphere_names(copy.deepcopy(ds))
        )

    def test_normalize_exc_name_no_category(self):
        ds = [{
            'exchanges': [{
                'type': "biosphere",
                'name': "Carbon dioxide, biogenic",
            }]
        }]
        self.assertEqual(
            ds,
            normalize_biosphere_names(copy.deepcopy(ds))
        )

    def test_missing_exc_name_raises_error(self):
        ds = [{
            'exchanges': [{
                'type': "biosphere",
                'categories': ['air'],
            }]
        }]
        with self.assertRaises(StrategyError):
            normalize_biosphere_names(ds)


class BiosphereCategoryNormalizationTestCase(unittest.TestCase):
    def test_no_categories(self):
        ds = [{'name': 'foo'}]
        self.assertEqual(
            copy.deepcopy(ds),
            normalize_biosphere_categories(ds)
        )

    def test_ds_wrong_type(self):
        ds = [{
            'categories': ('resource', 'in ground'),
            'type': 'process'
        }]
        self.assertEqual(
            copy.deepcopy(ds),
            normalize_biosphere_categories(ds)
        )

    def test_ds_categories_as_list(self):
        ds = [{
            'categories': ['resource', 'in ground'],
            'type': 'emission'
        }]
        expected = [{
            'categories': (u'natural resource', u'in ground'),
            'type': 'emission'
        }]
        self.assertEqual(
            expected,
            normalize_biosphere_categories(ds)
        )

    def test_ds(self):
        ds = [
            {
                'categories': ('resource', 'in ground'),
                'type': 'emission'
            },  {
                'categories': ('resource', 'all around'),
                'type': 'emission'
            }
        ]
        expected = [
            {
                'categories': (u'natural resource', u'in ground'),
                'type': 'emission'
            }, {
                'categories': ('resource', 'all around'),
                'type': 'emission'
            }
        ]
        self.assertEqual(
            expected,
            normalize_biosphere_categories(ds)
        )

    def test_exc_no_categories(self):
        ds = [{
            'exchanges': [{'name': 'foo'}]
        }]
        self.assertEqual(
            copy.deepcopy(ds),
            normalize_biosphere_categories(ds)
        )

    def test_exc_categories_as_list(self):
        ds = [{
            'exchanges': [{
                'categories': ['resource', 'in ground'],
                'type': 'biosphere',
            }]
        }]
        expected = [{
            'exchanges': [{
                'categories': (u'natural resource', u'in ground'),
                'type': 'biosphere',
            }]
        }]
        self.assertEqual(
            expected,
            normalize_biosphere_categories(ds)
        )

    def test_exc(self):
        ds = [{
            'exchanges': [
                {
                    'categories': ('resource', 'in ground'),
                    'type': 'biosphere',
                }, {
                    'categories': ('resource', 'all around'),
                    'type': 'biosphere'
                }
            ]
        }]
        expected = [{
            'exchanges': [
                {
                    'categories': (u'natural resource', u'in ground'),
                    'type': 'biosphere',
                }, {
                    'categories': ('resource', 'all around'),
                    'type': 'biosphere'
                }
            ]
        }]
        self.assertEqual(
            expected,
            normalize_biosphere_categories(ds)
        )
