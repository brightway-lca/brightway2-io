db_params = [{"name": "system_life_time_yr", "formula": "20 yr"}]

biosphere = {
    ("biosphere", "1"): {
        "categories": ["air", "urban-air from high stacks"],
        "code": "1",
        "exchanges": [],
        "name": "Carbon dioxide, fossil",
        "type": "emission",
        "unit": "kg",
    },
    ("biosphere", "2"): {
        "categories": ["air"],
        "code": "2",
        "exchanges": [],
        "type": "emission",
        "name": "Carbon dioxide, fossil",
        "unit": "kg",
    },
}

data = [
    {
        "name": "A",
        "location": "DE",
        "unit": "kilogram / year",
        "parameters": [
            {"name": "efficiency", "formula": "0.6"},
            {"name": "production_kg_per_yr", "formula": "1e6 kg/yr"},
        ],
        "exchanges": [
            {
                "name": "B",
                "location": "DE",
                "formula": "production_kg_per_yr * system_life_time_yr / efficiency",
                "type": "technosphere",
            },
            {
                "name": "C",
                "location": "FR",
                "formula": "2",
                "type": "technosphere",
            },
        ],
    },
    {
        "name": "B",
        "location": "DE",
        "unit": "kilogram",
        "exchanges": [
            {
                "name": "Carbon dioxide, fossil",
                "amount": 1,
                "unit": "kilogram",
                "categories": ("air", "urban-air from high stacks"),
                "type": "biosphere",
                "database": "biosphere",
            }
        ],
    },
    {
        "name": "C",
        "location": "FR",
        "unit": "unit",
        "exchanges": [],
    },
]
