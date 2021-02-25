SIMAPRO_BIO_SUBCATEGORIES = {
    "groundwater": "ground-",
    "groundwater, long-term": "ground-, long-term",
    "high. pop.": "urban air close to ground",
    "low. pop.": "non-urban air or from high stacks",
    "low. pop., long-term": "low population density, long-term",
    "stratosphere + troposphere": "lower stratosphere + upper troposphere",
    "river": "surface water",
    "river, long-term": "surface water",
    "lake": "surface water",
}

SIMAPRO_BIOSPHERE = {
    # LCI files
    "Economic issues": "economic",
    "Emissions to air": "air",
    "Emissions to soil": "soil",
    "Emissions to water": "water",
    "Non material emissions": "non-material",
    "Non mat.": "non-material",
    "Resources": "natural resource",
    "Social issues": "social",
    # LCIA files
    "Economic": "economic",
    "Air": "air",
    "Soil": "soil",
    "Water": "water",
    "Raw": "natural resource",
    "Waste": "waste",
}

SIMAPRO_SYSTEM_MODELS = {
    "apos": "Allocation, ecoinvent default",
    "consequential": "Substitution, consequential, long-term",
    "cutoff": "Allocation, cut-off by classification",
}

# Only includes combinations that changed from 2 to 3. Soil is unchanged.
ECOSPOLD_2_3_BIOSPHERE = {
    ("resource", "in ground"): ("natural resource", "in ground"),
    ("resource", "in air"): ("natural resource", "in air"),
    ("resource", "in water"): ("natural resource", "in water"),
    ("resource", "land"): ("natural resource", "land"),
    ("resource", "biotic"): ("natural resource", "biotic"),
    ("resource",): ("natural resource",),
    ("air", "high population density"): ("air", "urban air close to ground"),
    ("air", "low population density"): ("air", "non-urban air or from high stacks"),
    ("water", "fossil-"): ("water", "ground-"),
    ("water", "lake"): ("water", "surface water"),
    ("water", "river"): ("water", "surface water"),
    ("water", "river, long-term"): ("water", "surface water"),
}
