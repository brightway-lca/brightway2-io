UNITS_NORMALIZATION = {
    "ha": u"hectare",
    "hr": u"hour",
    "kbq": u"kilo Becquerel",
    "kg": u"kilogram",
    "km": u"kilometer",
    "kwh": u"kilowatt hour",
    "m*year": u"meter-year",
    "m2": u"square meter",
    "m2a": u"square meter-year",
    "m3": u"cubic meter",
    "m3a": u"cubic meter-year",
    "ma": u"meter-year",
    "metric ton*km": u"ton kilometer",
    "mj": u"megajoule",
    "my": u"meter-year",
    "nm3": u"cubic meter",
    "p": u"unit",
    "personkm": u"person kilometer",
    "pkm": u"person kilometer",
    "tkm": u"ton kilometer",
    "vkm": u"vehicle kilometer",
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(x.lower(), x)



