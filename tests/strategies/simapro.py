# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.strategies.simapro import *

def test_localized_water_flows():
    given = [{'exchanges': [{
        'type': 'foo',
        'name': 'Water, BR',
    }, {
        'input': True,
        'name': 'Water, BR',
    }, {
        'type': 'biosphere',
        'name': 'Not Water, BR'
    }, {
        'type': 'biosphere',
        'name': "Water, turbine use, unspecified natural origin, UCTE without Germany and France"
    }, {
        'type': 'biosphere',
        'name': "Water, river, Québec, HQ distribution network"
    }, {
        'type': 'biosphere',
        'name': "Water, well, in ground, IAI Area 4&5, without China"
    }, {
        'type': 'biosphere',
        'name': "Water, unspecified natural origin, HU"
    }]}]
    expected = [{'exchanges': [{
        'type': 'foo',
        'name': 'Water, BR',
    }, {
        'input': True,
        'name': 'Water, BR',
    }, {
        'type': 'biosphere',
        'name': 'Not Water, BR'
    }, {
        'type': 'biosphere',
        'name': "Water, turbine use, unspecified natural origin",
        'simapro location': "UCTE without Germany and France"
    }, {
        'type': 'biosphere',
        'name': "Water, river",
        'simapro location': 'Québec, HQ distribution network'
    }, {
        'type': 'biosphere',
        'name': "Water, well, in ground",
        'simapro location': 'IAI Area 4&5, without China'
    }, {
        'type': 'biosphere',
        'name': "Water, unspecified natural origin",
        'simapro location': 'HU'
    }]}]
    assert fix_localized_water_flows(given) == expected


def test_change_electricity_units():
    given = [{'exchanges': [{
        'name': 'market for electricity, etc.',
        'unit': 'kilowatt hour',
        'amount': 1
    }, {
        'name': 'electricity, blah blah blah',
        'unit': 'megajoule',
        'amount': 7.2
    }, {
        'name': 'market for electricity, do be do be dooooo',
        'unit': 'megajoule',
        'amount': 3.6
    }]}]
    expected = [{'exchanges': [{
        'name': 'market for electricity, etc.',
        'unit': 'kilowatt hour',
        'amount': 1,
    }, {
        'name': 'electricity, blah blah blah',
        'unit': 'kilowatt hour',
        'amount': 2,
        'loc': 2,
    }, {
        'name': 'market for electricity, do be do be dooooo',
        'unit': 'kilowatt hour',
        'amount': 1,
        'loc': 1,
    }]}]
    assert change_electricity_unit_mj_to_kwh(given) == expected
