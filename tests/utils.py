# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.utils import (
    activity_hash,
    es2_activity_hash,
    format_for_logging,
    load_json_data_file,
    standardize_method_to_len_3,
)
import unittest
import sys


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
            'd41d8cd98f00b204e9800998ecf8427e'
        )
        self.assertEqual(
            activity_hash({}),
            'd41d8cd98f00b204e9800998ecf8427e'
        )
        self.assertTrue(isinstance(activity_hash({}), str))
        ds = {
            'name': 'care bears',
            'unit': 'kilogram',
            'location': 'GLO'
        }
        self.assertEqual(
            activity_hash(ds),
            'a6d6dd46cc33acd23826fa5b4e83377f'
        )
        ds = {
            'name': 'care bears',
            'categories': ['toys', 'fun'],
            'unit': 'kilogram',
            'reference product': 'lollipops',
            'location': 'GLO',
            'extra': 'irrelevant',
        }
        self.assertEqual(
            activity_hash(ds),
            '90d8689ec08dceb9507d28a36df951cd'
        )
        ds = {'name': '正しい馬のバッテリーの定番'}
        self.assertEqual(
            activity_hash(ds),
            'd2b18b4f9f9f88189c82224ffa524e93'
        )

    def test_format_for_logging(self):
        ds = {
            'name': 'care bears',
            'unit': 'kilogram',
            'location': 'GLO'
        }
        if sys.version_info < (3, 0):
            answer = b"{ u'location': u'GLO', u'name': u'care bears', u'unit': u'kilogram'}"
        else:
            answer = "{'location': 'GLO', 'name': 'care bears', 'unit': 'kilogram'}"
        self.assertEqual(
            format_for_logging(ds),
            answer
        )

    def test_es2_activity_hash(self):
        ds = ('foo', 'bar')
        self.assertEqual(
            es2_activity_hash(*ds),
            '3858f62230ac3c915f300c664312c63f'
        )
        self.assertTrue(isinstance(es2_activity_hash(*ds), str))
        ds = (u"正しい馬", u"バッテリーの定番")
        self.assertEqual(
            es2_activity_hash(*ds),
            '008e9536b44699d8b0d631d9acd76515'
        )


def test_standardize_method_to_len_3():
    a = ("foo", "bar")
    b = ()
    c = tuple("abcde")
    d = tuple("abc")
    e = list("ab")
    f = list("abcde")

    assert standardize_method_to_len_3(a) == ("foo", "bar", "--")
    assert standardize_method_to_len_3(a, "##") == ("foo", "bar", "##")
    assert standardize_method_to_len_3(b) == ("--", "--", "--")
    assert standardize_method_to_len_3(c) == ("a", "b", "c,d,e")
    assert standardize_method_to_len_3(c, joiner="; ") == ("a", "b", "c; d; e")
    assert standardize_method_to_len_3(d) == ("a", "b", "c")
    assert standardize_method_to_len_3(e) == ("a", "b", "--")
    assert standardize_method_to_len_3(f) == ("a", "b", "c,d,e")
