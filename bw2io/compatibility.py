SIMAPRO_BIO_SUBCATEGORIES = {
    "groundwater": u'ground-',
    "groundwater, long-term": u'ground-, long-term',
    "high. pop.": u'urban air close to ground',
    "low. pop.": u'non-urban air or from high stacks',
    "low. pop., long-term": u'low population density, long-term',
    "stratosphere + troposphere": u'lower stratosphere + upper troposphere',
    'river': u'surface water',
    'river, long-term': u'surface water',
    'lake': u'surface water',
}

SIMAPRO_BIOSPHERE = {
    # LCI files
    "Economic issues": u"economic",
    "Emissions to air": u"air",
    "Emissions to soil": u"soil",
    "Emissions to water": u"water",
    "Non material emissions": u"non-material",
    "Non mat.": u"non-material",
    "Resources": u"natural resource",
    "Social issues": u"social",
    # LCIA files
    "Economic": u"economic",
    "Air": u"air",
    "Soil": u"soil",
    "Water": u"water",
    "Raw": u"natural resource",
    "Waste": u"waste",
}

SIMAPRO_SYSTEM_MODELS = {
    "apos": u"Allocation, ecoinvent default",
    "consequential": u"Substitution, consequential, long-term",
    "cutoff": u"Allocation, cut-off by classification",
}

# Only includes combinations that changed from 2 to 3. Soil is unchanged.
ECOSPOLD_2_3_BIOSPHERE = {
    ('resource', 'in ground'): (u'natural resource', u'in ground'),
    ('resource', 'in air'): (u'natural resource', u'in air'),
    ('resource', 'in water'): (u'natural resource', u'in water'),
    ('resource', 'land'): (u'natural resource', u'land'),
    ('resource', 'biotic'): (u'natural resource', u'biotic'),
    ('resource', ): (u'natural resource', ),
    ('air', 'high population density'): (u'air', u'urban air close to ground'),
    ('air', 'low population density'): (u'air', u'non-urban air or from high stacks'),
    ('water', 'fossil-'): (u'water', u'ground-'),
    ('water', 'lake'): (u'water', u'surface water'),
    ('water', 'river'): (u'water', u'surface water'),
    ('water', 'river, long-term'): (u'water', u'surface water'),
}
