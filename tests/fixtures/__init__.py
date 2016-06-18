# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

biosphere = {
    ("biosphere", 1): {
        'categories': ['things'],
        'code': 1,
        'exchanges': [],
        'name': 'an emission',
        'type': 'emission',
        'unit': 'kg'
        },
    ("biosphere", 2): {
        'categories': ['things'],
        'code': 2,
        'exchanges': [],
        'type': 'emission',
        'name': 'another emission',
        'unit': 'kg'
        },
}

food = {
    ("food", 1): {
        'categories': ['stuff', 'meals'],
        'code': 1,
        'exchanges': [{
            'amount': 0.5,
            'input': ('food', 2),
            'type': 'technosphere',
            'uncertainty type': 0},
            {'amount': 0.05,
            'input': ('biosphere', 1),
            'type': 'biosphere',
            'uncertainty type': 0}],
        'location': 'CA',
        'name': 'lunch',
        'type': 'process',
        'unit': 'kg'
        },
    ("food", 2): {
        'categories': ['stuff', 'meals'],
        'code': 2,
        'exchanges': [{
            'amount': 0.25,
            'input': ('food', 1),
            'type': 'technosphere',
            'uncertainty type': 0},
            {'amount': 0.15,
            'input': ('biosphere', 2),
            'type': 'biosphere',
            'uncertainty type': 0}],
        'location': 'CH',
        'name': 'dinner',
        'type': 'process',
        'unit': 'kg'
        },
    }

food2 = {
    ("food", "1"): {
        'categories': ['stuff', 'meals'],
        'code': 1,
        'exchanges': [{
            'amount': 0.5,
            'input': ('food', 2),
            'type': 'technosphere',
            'uncertainty type': 0},
            {'amount': 0.05,
            'input': ('biosphere', 1),
            'type': 'biosphere',
            'uncertainty type': 0}],
        'location': 'CA',
        'name': 'lunch',
        'type': 'process',
        'unit': 'kg'
        },
    ("food", "2"): {
        'categories': ['stuff', 'meals'],
        'code': 2,
        'exchanges': [{
            'amount': 0.25,
            'input': ('food', 1),
            'type': 'technosphere',
            'uncertainty type': 0},
            {'amount': 0.15,
            'input': ('biosphere', 2),
            'type': 'biosphere',
            'uncertainty type': 0}],
        'location': 'CH',
        'name': 'dinner',
        'type': 'process',
        'unit': 'kg'
        },
    }
