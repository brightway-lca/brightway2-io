SIMAPRO_BIO_SUBCATEGORIES = {
    u"groundwater": u'ground-',
    u"groundwater, long-term": u'ground-, long-term',
    u"high. pop.": u'urban air close to ground',
    u"low. pop.": u'non-urban air or from high stacks',
    u"low. pop., long-term": u'low population density, long-term',
    u"stratosphere + troposphere": u'lower stratosphere + upper troposphere',
    u'river': u'surface water',
    u'river, long-term': u'surface water',
    u'lake': u'surface water',
    u'(unspecified)': u'unspecified',
}

SIMAPRO_BIOSPHERE = {
    u"Economic issues": u"economic",
    u"Emissions to air": u"air",
    u"Emissions to soil": u"soil",
    u"Emissions to water": u"water",
    u"Non material emissions": u"non-material",
    u"Resources": u"natural resource",
    u"Social issues": u"social",
}

SIMAPRO_SYSTEM_MODELS = {
    "apos": "Allocation, ecoinvent default",
    "consequential": "Substitution, consequential, long-term",
    "cutoff": "Allocation, cut-off by classification",
}

ECOSPOLD_2_3_BIOSPHERE = {
   ('resource', 'in ground'): (u'natural resource', u'in ground'),
   ('resource', 'in air'): (u'natural resource', u'in air'),
   ('resource', 'in water'): (u'natural resource', u'in water'),
   ('resource', 'land'): (u'natural resource', u'land'),
   ('resource', 'biotic'): (u'natural resource', u'biotic'),
   ('air', 'high population density'): (u'air', u'urban air close to ground'),
   ('air', 'low population density'): (u'air', u'non-urban air or from high stacks'),
   ('air', 'low population density, long-term'): (u'air', u'low population density, long-term'),
   ('air', 'land'): (u'air', u'land'),
}
