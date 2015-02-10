# -*- coding: utf-8 -*
from ..utils import (
    activity_hash,
    es2_activity_hash,
    format_for_logging,
    load_json_data_file,
)
import unittest

class UtilsTestCase(unittest.TestCase):
    def test_load_json_data(self):
        self.assertEqual(
            {"1": {"2": 3}},
            load_json_data_file("test")
        )
        self.assertEqual(
            {"1": {"2": 3}},
            load_json_data_file("test.json")
        )

    def test_activity_hash(self):
        self.assertEqual(
            activity_hash({}),
            u'd41d8cd98f00b204e9800998ecf8427e'
        )
        self.assertEqual(
            activity_hash({}),
            u'd41d8cd98f00b204e9800998ecf8427e'
        )
        self.assertTrue(isinstance(activity_hash({}), unicode))
        ds = {
            u'name': u'care bears',
            u'unit': u'kilogram',
            u'location': u'GLO'
        }
        self.assertEqual(
            activity_hash(ds),
            u'a6d6dd46cc33acd23826fa5b4e83377f'
        )
        ds = {
            u'name': u'care bears',
            u'categories': [u'toys', u'fun'],
            u'unit': u'kilogram',
            u'reference product': u'lollipops',
            u'location': u'GLO',
            u'extra': u'irrelevant',
        }
        self.assertEqual(
            activity_hash(ds),
            u'90d8689ec08dceb9507d28a36df951cd'
        )
        ds = {u'name': u'正しい馬のバッテリーの定番'}
        self.assertEqual(
            activity_hash(ds),
            u'd2b18b4f9f9f88189c82224ffa524e93'
        )

    def test_format_for_logging(self):
        ds = {
            u'name': u'care bears',
            u'unit': u'kilogram',
            u'location': u'GLO'
        }
        self.assertEqual(
            format_for_logging(ds),
            u"{ u'location': u'GLO', u'name': u'care bears', u'unit': u'kilogram'}",
        )

    def test_es2_activity_hash(self):
        ds = (u'foo', u'bar')
        self.assertEqual(
            es2_activity_hash(*ds),
            u'3858f62230ac3c915f300c664312c63f'
        )
        self.assertTrue(isinstance(es2_activity_hash(*ds), unicode))
        ds = (u"正しい馬", u"バッテリーの定番")
        self.assertEqual(
            es2_activity_hash(*ds),
            u'008e9536b44699d8b0d631d9acd76515'
        )
