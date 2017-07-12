# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals, division
from eight import *

UNITS_NORMALIZATION = {
    "a": "year",  # Common in LCA circles; could be confused with are
    "bq": "Becquerel",
    "g": "gram",
    "gj": "gigajoule",
    "h": "hour",
    "ha": "hectare",
    "hr": "hour",
    "kbq": "kilo Becquerel",
    "kg": "kilogram",
    "kgkm": "kilogram kilometer",
    "km": "kilometer",
    "kj": "kilojoule",
    "kwh": "kilowatt hour",
    "l": "litre",
    "lu": "livestock unit",
    "m": "meter",
    "m*year": "meter-year",
    "m2": "square meter",
    "m2*year": "square meter-year",
    "m2a": "square meter-year",
    "m2y": "square meter-year",
    "m3": "cubic meter",
    "m3*year": "cubic meter-year",
    "m3a": "cubic meter-year",
    "m3y": "cubic meter-year",
    "ma": "meter-year",
    "metric ton*km": "ton kilometer",
    "mj": "megajoule",
    "my": "meter-year",
    "nm3": "cubic meter",
    "p": "unit",
    "personkm": "person kilometer",
    "person*km": "person kilometer",
    "pkm": "person kilometer",
    "tkm": "ton kilometer",
    "vkm": "vehicle kilometer",
    'kg sw': "kilogram separative work unit",
    'km*year': "kilometer-year",
    'metric ton*km': "ton kilometer",
    'person*km': "person kilometer",
    'wh': 'watt hour',
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(x.lower(), x)

DEFAULT_UNITS_CONVERSION = [
    # Energy
    ('PJ', 'megajoule', 1e9),
    ('J', 'megajoule', 1e-06),
    ('Nm3 Europe', 'megajoule', 37.5),
    ('Bt', 'megajoule', 0.001055696),
    ('kilojoule', 'megajoule', 1e-3),
    ('kg-NL', 'megajoule', 42.4),
    ('Nm3-Russia', 'megajoule', 36.4),
    ('Nm3-DZ', 'megajoule', 38.5),
    ('Nm3-GB', 'megajoule', 37.0),
    ('Nm3 - NG', 'megajoule', 38.5),
    ('Nm3- Italy', 'megajoule', 33.85),
    ('Nm3 F-v6', 'megajoule', 36.894),
    ('kg-DZ', 'megajoule', 49.7),
    ('kWp', 'megajoule', 3.6),
    ('kg-DE', 'megajoule', 45.9),
    ('gigajoule', 'megajoule', 1e3),
    ('kcal', 'megajoule', 0.0041855),
    ('kg-RU', 'megajoule', 49.3),
    ('kg-NO', 'megajoule', 56.6),
    ('TJ', 'megajoule', 1e6),
    ('Nm3-DE', 'megajoule', 35.0),
    ('Nm3 FR', 'megajoule', 37.09),
    ('Nm3 - EG', 'megajoule', 38.5),
    ('Nm3-NL', 'megajoule', 34.9),
    ('kg-GB', 'megajoule', 47.0),
    ('NM3-Norway', 'megajoule', 40.8),
    ('cal', 'megajoule', 4.19e-06),

    ('MWh', 'kilowatt hour', 1e3),
    ('TWh', 'kilowatt hour', 1e9),
    ('watt hour', 'kilowatt hour', 1e-3),
    ('GWh', 'kilowatt hour', 1e6),

    # Mass
    ('gram', 'kilogram', 1e-3),
    ('t', 'kilogram', 1e3),
    ('tMS', 'kilogram', 1e3),
    ('lb', 'kilogram', 0.4535924),
    ('T', 'kilogram', 1e3),
    ('tn.sh', 'kilogram', 907.1848),
    ('tMB', 'kilogram', 1e3),
    ('pg', 'kilogram', 1e-15),
    ('mg', 'kilogram', 1e-06),
    ('kton', 'kilogram', 1e6),
    ('oz', 'kilogram', 0.02834952),
    ('?g', 'kilogram', 1e-09),  # SimaPro weirdness
    ('μg', 'kilogram', 1e-09),
    ('tn.lg', 'kilogram', 1016.047),
    ('tOM', 'kilogram', 1e3),
    ('Mtn', 'kilogram', 1e9),
    ('ton', 'kilogram', 1e3),
    ('ng', 'kilogram', 1e-12),

    # Length
    ('yard', 'meter', 0.9144),
    ('inch', 'meter', 0.0254),
    ('mm', 'meter', 1e-3),
    # ('kilometer', 'meter', 1e3),
    ('dm', 'meter', 0.1),
    ('Nautic mil', 'meter', 1547.896),
    ('mile', 'meter', 1609.35),
    ('cm', 'meter', 0.01),
    ('ft', 'meter', 0.3048),
    ('?m', 'meter', 1e-06),
    ('μm', 'meter', 1e-06),

    # Length * time
    ('miy', 'meter-year', 1609.35),
    ('kmy', 'meter-year', 1e3),

    # Volume
    ('dm3', 'cubic meter', 1e-3),
    ('cu.in', 'cubic meter', 1.638706e-05),
    ('cuft', 'cubic meter', 0.02831685),
    ('mm3', 'cubic meter', 1e-09),
    ('gal*', 'cubic meter', 0.003785412),  # SimaPro weirdness
    ('cu.yd', 'cubic meter', 0.7645549),
    ('cm3', 'cubic meter', 1e-6),
    # ('litre', 'cubic meter', 1e-3),

    # Volume * time
    ('l*day', 'cubic meter-year', 2.7397e-06),
    ('cm3y', 'cubic meter-year', 1e-06),
    ('m3day', 'cubic meter-year', 0.0027397),

    # Area
    ('sq.mi', 'square meter', 2589988.0),
    ('sq.ft', 'square meter', 0.09290304),
    ('sq.in', 'square meter', 0.00064516),
    ('sq.yd', 'square meter', 0.8361273),
    ('cm2', 'square meter', 0.0001),
    ('km2', 'square meter', 1e6),
    ('dm2', 'square meter', 0.01),
    ('acre', 'square meter', 4046.856),
    ('mm2', 'square meter', 1e-06),

    # Area * time
    ('mm2a', 'square meter-year', 1e-06),
    ('ha a', 'square meter-year', 1e4),
    ('cm2a', 'square meter-year', 1e-4),
    ('km2a', 'square meter-year', 1e6),

    # Radiation
    ('nBq', 'kilo Becquerel', 1e-12),
    ('Becquerel', 'kilo Becquerel', 1e-3),
    ('MBq', 'kilo Becquerel', 1e3),
    ('?Bq', 'kilo Becquerel', 1e-9),  # SimaPro weirdness
    ('μBq', 'kilo Becquerel', 1e-9),
    ('mBq', 'kilo Becquerel', 1e-6),

    # Time
    ('s', 'hour', 3600.0),
    ('day', 'hour', 1 / 24),
    ('year', 'hour', 1 / 8765.81),
    ('min', 'hour', 60.0),

    # Sector-specific
    ('pmi', 'person kilometer', 1.60935),
    ('kilogram kilometer', 'ton kilometer', 1e-3),
    ('tmi*', 'ton kilometer', 1.45997),
    ('ktkm', 'ton kilometer', 1e3),
]

def get_default_units_migration_data():
    return {
        'fields': ['unit'],
        'data': [(
            (from_unit,),
            {'unit': to_unit, 'multiplier': multiplier}
        ) for from_unit, to_unit, multiplier in DEFAULT_UNITS_CONVERSION]
    }

_USED_IN_ECOINVENT = {
    'cubic meter',
    'hectare',
    'hour',
    'kg SWU',
    'kilogram',
    'kilometer',
    'kilometer-year',
    'kilowatt hour',
    'litre',
    'livestock unit',
    'megajoule',
    'meter',
    'meter-year',
    'person kilometer',
    'pig place',
    'square meter',
    'square meter-year',
    'ton kilometer',
    'unit',
    'vehicle kilometer'
}

def get_unusual_units_migration_data():
    """Only convert units that are not used in ecoinvent at all"""
    return {
        'fields': ['unit'],
        'data': [(
            (from_unit,),
            {'unit': to_unit, 'multiplier': multiplier}
        ) for from_unit, to_unit, multiplier in DEFAULT_UNITS_CONVERSION
        if from_unit not in _USED_IN_ECOINVENT]
    }
