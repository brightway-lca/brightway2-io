from ...strategies.simapro import (
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
)
from ...compatibility import SIMAPRO_BIOSPHERE, SIMAPRO_BIO_SUBCATEGORIES
import unittest


class SPNormalizationTestCase(unittest.TestCase):
    def biosphere_test_case(self):
        db = [{
            'exchanges': [{
                'categories': ["Economic issues", "foo"],
                'type': 'biosphere'
            }, {
                'categories': ["Resources", "high. pop."],
                'type': 'biosphere'
            }, {
                'categories': ["Economic issues", "high. pop."],
                'type': 'not biosphere'
            }],
        }]
        result = [{
            'exchanges': [{
                'categories': ("economic", "foo"),
                'type': 'biosphere'
            }, {
                'categories': ("natural resource", 'urban air close to ground'),
                'type': 'biosphere'
            }, {
                'categories': ["Economic issues", "high. pop."],
                'type': 'not biosphere'
            }],
        }]
        self.assertEqual(
            result,
            normalize_simapro_biosphere_categories(db),
        )
