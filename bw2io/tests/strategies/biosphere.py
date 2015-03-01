from ...strategies import (
    link_biosphere_by_activity_hash,
    normalize_biosphere_categories,
    normalize_biosphere_names,
)
from ...errors import StrategyError
import copy
import unittest


class BiosphereNormalizationTestCase(unittest.TestCase):
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

