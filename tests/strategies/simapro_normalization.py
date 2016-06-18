# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.strategies.simapro import (
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
)
from bw2io.compatibility import SIMAPRO_BIOSPHERE, SIMAPRO_BIO_SUBCATEGORIES
import unittest


class SPNormalizationTestCase(unittest.TestCase):
    def test_sp_biosphere_category_normalization(self):
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
