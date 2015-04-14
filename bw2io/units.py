# -*- coding: utf-8 -*

UNITS_NORMALIZATION = {
    "bq": u"Becquerel",
    "g": u"gram",
    "gj": u"gigajoule",
    "h": u"hour",
    "ha": u"hectare",
    "hr": u"hour",
    "kbq": u"kilo Becquerel",
    "kg": u"kilogram",
    "kgkm": u"kilogram kilometer",
    "km": u"kilometer",
    "kj": u"kilojoule",
    "kwh": u"kilowatt hour",
    "l": u"litre",
    "lu": u"livestock unit",
    "m": u"meter",
    "m*year": u"meter-year",
    "m2": u"square meter",
    "m2*year": u"square meter-year",
    "m2a": u"square meter-year",
    "m2y": u"square meter-year",
    "m3": u"cubic meter",
    "m3*year": u"cubic meter-year",
    "m3a": u"cubic meter-year",
    "m3y": u"cubic meter-year",
    "ma": u"meter-year",
    "metric ton*km": u"ton kilometer",
    "mj": u"megajoule",
    "my": u"meter-year",
    "nm3": u"cubic meter",
    "p": u"unit",
    "personkm": u"person kilometer",
    "person*km": u"person kilometer",
    "pkm": u"person kilometer",
    "tkm": u"ton kilometer",
    "vkm": u"vehicle kilometer",
    'kg swu': u"kilogram separative work unit",
    'km*year': u"kilometer-year",
    'metric ton*km': u"ton kilometer",
    'person*km': u"person kilometer",
    'wh': u'watt hour',
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(x.lower(), x)

DEFAULT_UNITS_CONVERSION = [
    # Energy
    ('PJ', u'megajoule', 1e9),
    ('J', u'megajoule', 1e-06),
    ('Nm3 Europe', u'megajoule', 37.5),
    ('Btu', u'megajoule', 0.001055696),
    (u'kilojoule', u'megajoule', 1e-3),
    ('kg-NL', u'megajoule', 42.4),
    ('Nm3-Russia', u'megajoule', 36.4),
    ('Nm3-DZ', u'megajoule', 38.5),
    ('Nm3-GB', u'megajoule', 37.0),
    ('Nm3 - NG', u'megajoule', 38.5),
    ('Nm3- Italy', u'megajoule', 33.85),
    ('Nm3 F-v6', u'megajoule', 36.894),
    ('kg-DZ', u'megajoule', 49.7),
    ('kWp', u'megajoule', 3.6),
    ('kg-DE', u'megajoule', 45.9),
    (u'gigajoule', u'megajoule', 1e3),
    ('kcal', u'megajoule', 0.0041855),
    ('kg-RU', u'megajoule', 49.3),
    ('kg-NO', u'megajoule', 56.6),
    ('TJ', u'megajoule', 1e6),
    ('Nm3-DE', u'megajoule', 35.0),
    ('Nm3 FR', u'megajoule', 37.09),
    ('Nm3 - EG', u'megajoule', 38.5),
    ('Nm3-NL', u'megajoule', 34.9),
    ('kg-GB', u'megajoule', 47.0),
    ('NM3-Norway', u'megajoule', 40.8),
    ('cal', u'megajoule', 4.19e-06),

    ('MWh', u'kilowatt hour', 1e3),
    ('TWh', u'kilowatt hour', 1e9),
    (u'watt hour', u'kilowatt hour', 1e-3),
    ('GWh', u'kilowatt hour', 1e6),

    # Mass
    (u'gram', u'kilogram', 1e-3),
    ('t', u'kilogram', 1e3),
    ('tMS', u'kilogram', 1e3),
    ('lb', u'kilogram', 0.4535924),
    ('T', u'kilogram', 1e3),
    ('tn.sh', u'kilogram', 907.1848),
    ('tMB', u'kilogram', 1e3),
    ('pg', u'kilogram', 1e-15),
    ('mg', u'kilogram', 1e-06),
    ('kton', u'kilogram', 1e6),
    ('oz', u'kilogram', 0.02834952),
    ('?g', u'kilogram', 1e-09),  # SimaPro weirdness
    (u'μg', u'kilogram', 1e-09),
    ('tn.lg', u'kilogram', 1016.047),
    ('tOM', u'kilogram', 1e3),
    ('Mtn', u'kilogram', 1e9),
    ('ton', u'kilogram', 1e3),
    ('ng', u'kilogram', 1e-12),

    # Length
    ('yard', u'meter', 0.9144),
    ('inch', u'meter', 0.0254),
    ('mm', u'meter', 1e-3),
    (u'kilometer', u'meter', 1e3),
    ('dm', u'meter', 0.1),
    ('Nautic mil', u'meter', 1547.896),
    ('mile', u'meter', 1609.35),
    ('cm', u'meter', 0.01),
    ('ft', u'meter', 0.3048),
    ('?m', u'meter', 1e-06),
    (u'μm', u'meter', 1e-06),

    # Length * time
    ('miy', u'meter-year', 1609.35),
    ('kmy', u'meter-year', 1e3),

    # Volume
    ('dm3', u'cubic meter', 1e-3),
    ('cu.in', u'cubic meter', 1.638706e-05),
    ('cuft', u'cubic meter', 0.02831685),
    ('mm3', u'cubic meter', 1e-09),
    ('gal*', u'cubic meter', 0.003785412),  # SimaPro weirdness
    ('cu.yd', u'cubic meter', 0.7645549),
    ('cm3', u'cubic meter', 1e-6),
    (u'litre', u'cubic meter', 1e-3),

    # Volume * time
    ('l*day', u'cubic meter-year', 2.7397e-06),
    ('cm3y', u'cubic meter-year', 1e-06),
    ('m3day', u'cubic meter-year', 0.0027397),

    # Area
    ('sq.mi', u'square meter', 2589988.0),
    ('sq.ft', u'square meter', 0.09290304),
    ('sq.in', u'square meter', 0.00064516),
    ('sq.yd', u'square meter', 0.8361273),
    ('cm2', u'square meter', 0.0001),
    ('km2', u'square meter', 1e6),
    ('dm2', u'square meter', 0.01),
    ('acre', u'square meter', 4046.856),
    ('mm2', u'square meter', 1e-06),

    # Area * time
    ('mm2a', u'square meter-year', 1e-06),
    ('ha a', u'square meter-year', 1e4),
    ('cm2a', u'square meter-year', 1e-4),
    ('km2a', u'square meter-year', 1e6),

    # Radiation
    ('nBq', u'kilo Becquerel', 1e-12),
    (u'Becquerel', u'kilo Becquerel', 1e-3),
    ('MBq', u'kilo Becquerel', 1e3),
    ('?Bq', u'kilo Becquerel', 1e-9),  # SimaPro weirdness
    (u'μBq', u'kilo Becquerel', 1e-9),
    ('mBq', u'kilo Becquerel', 1e-6),

    # Time
    ('s', u'hour', 3600.0),
    ('day', u'hour', 1 / 12.),
    ('year', u'hour', 1 / 8765.81),
    ('min', u'hour', 60.0),

    # Sector-specific
    ('pmi', u'person kilometer', 1.60935),
    (u'kilogram kilometer', u'ton kilometer', 1e-3),
    ('tmi*', u'ton kilometer', 1.45997),
    ('ktkm', u'ton kilometer', 1e3),
]

def get_default_units_migration_data():
    return {
        'fields': ['unit'],
        'data': [(
            (from_unit,),
            {'unit': to_unit, 'multiplier': multiplier}
        ) for from_unit, to_unit, multiplier in DEFAULT_UNITS_CONVERSION]
    }
