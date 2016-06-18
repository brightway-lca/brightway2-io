# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.strategies.simapro import detoxify_re, split_simapro_name_geo
import unittest


class NameSplittingTestCase(unittest.TestCase):
    def test_detoxify_re(self):
        self.assertFalse(detoxify_re.search("Cheese U"))
        self.assertFalse(detoxify_re.search("Cheese/CH"))
        self.assertTrue(detoxify_re.search("Cheese/CH U"))
        self.assertTrue(detoxify_re.search("Cheese/CH/I U"))
        self.assertTrue(detoxify_re.search("Cheese/CH/I S"))
        self.assertTrue(detoxify_re.search("Cheese/RER U"))
        self.assertTrue(detoxify_re.search("Cheese/CENTREL U"))
        self.assertTrue(detoxify_re.search("Cheese/CENTREL S"))

    def test_detoxify_re2(self):
        test_strings = [
            u'Absorption chiller 100kW/CH/I U',
            u'Disposal, solvents mixture, 16.5% water, to hazardous waste incineration/CH U',
            u'Electricity, at power plant/hard coal, IGCC, no CCS/2025/RER U',
            u'Electricity, natural gas, at fuel cell SOFC 200kWe, alloc exergy, 2030/CH U',
            u'Heat exchanger/of cogen unit 160kWe/RER/I U',
            u'Lignite, burned in power plant/post, pipeline 200km, storage 1000m/2025/RER U',
            u'Transport, lorry >28t, fleet average/CH U',
            u'Water, cooling, unspecified natural origin, CH',
            u'Water, cooling, unspecified natural origin/m3',
            u'Water/m3',
        ]

        expected_results = [
            [(u'Absorption chiller 100kW', u'CH', u'/I')],
            [(u'Disposal, solvents mixture, 16.5% water, to hazardous waste incineration', u'CH', u'')],
            [(u'Electricity, at power plant/hard coal, IGCC, no CCS/2025', u'RER', u'')],
            [(u'Electricity, natural gas, at fuel cell SOFC 200kWe, alloc exergy, 2030', u'CH', u'')],
            [(u'Heat exchanger/of cogen unit 160kWe', u'RER', u'/I')],
            [(u'Lignite, burned in power plant/post, pipeline 200km, storage 1000m/2025', u'RER', u'')],
            [(u'Transport, lorry >28t, fleet average', u'CH', u'')],
            [], [], []
        ]
        for string, result in zip(test_strings, expected_results):
            self.assertEqual(detoxify_re.findall(string), result)

    def test_splitting_datasets(self):
        db = [
            {'name': 'Absorption chiller 100kW/CH/I U'},
            {'name': 'Cheese/CH'},
        ]
        result = [
            {
                'name': 'Absorption chiller 100kW',
                'location': 'CH',
                'reference product': 'Absorption chiller 100kW',
                'simapro name': 'Absorption chiller 100kW/CH/I U',

            },
            {'name': 'Cheese/CH'},
        ]
        self.assertEqual(split_simapro_name_geo(db), result)

    def test_splitting_exchanges(self):
        db = [{
            'name': 'foo',
            'exchanges': [{
                'name': 'Absorption chiller 100kW/CH/I U'
            }, {
                'name': 'Cheese/CH'
            }],
        }]
        result = [{
            'name': 'foo',
            'exchanges': [{
                'name': 'Absorption chiller 100kW',
                'location': 'CH',
                'simapro name': 'Absorption chiller 100kW/CH/I U',
            }, {
                'name': 'Cheese/CH'
            }],
        }]
        self.assertEqual(split_simapro_name_geo(db), result)
