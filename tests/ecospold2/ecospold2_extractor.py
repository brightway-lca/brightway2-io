from bw2io.extractors.ecospold2 import Ecospold2DataExtractor
import os

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures", "ecospold2")


def test_extraction_without_synonyms():
    data = Ecospold2DataExtractor.extract(
        os.path.join(
            FIXTURES,
            "00000_11111111-2222-3333-4444-555555555555_66666666-7777-8888-9999-000000000000.spold",
        ),
        "ei",
    )
    expected = {
        "comment": "Things and stuff and whatnot\nTechnology:  typical technology for ze Germans!",
        "classifications": [
            ("EcoSpold01Categories", "construction materials/concrete"),
            (
                "ISIC rev.4 ecoinvent",
                "2395:Manufacture of articles of concrete, cement and plaster",
            ),
        ],
        "activity type": "ordinary transforming activity",
        "activity": "c40e3c0a-292f-45a5-88cd-ed18265cb7d7",
        "database": "ei",
        "exchanges": [
            {
                "flow": "6fe4040b-39c7-4d58-b95b-6ee1de4aedb3",
                "type": "technosphere",
                "name": "clay pit infrastructure",
                "classifications": {
                    "CPC": ["53269: Other constructions for manufacturing"]
                },
                "production volume": 0.0,
                "properties": {},
                "activity": "759fac54-b912-4781-9833-0ddd6e8cda24",
                "unit": "unit",
                "comment": "estimated",
                "amount": 9999.0,
                "pedigree": {
                    "reliability": 1,
                    "completeness": 1,
                    "temporal correlation": 5,
                    "geographical correlation": 1,
                    "further technological correlation": 1,
                },
                "uncertainty type": 2,
                "loc": 0.0,
                "scale": 0.4472135954999579,
                "scale without pedigree": 0.31622776601683794,
            },
            {
                "flow": "d4ee8f39-342b-4443-bbb9-c49b6801b5d6",
                "type": "production",
                "name": "concrete block",
                "classifications": {
                    "CPC": ["37510: Non-refractory mortars and concretes"]
                },
                "production volume": 42.0,
                "properties": {
                    "carbon content, non-fossil": {
                        "amount": 0.0,
                        "unit": "dimensionless",
                    },
                    "water content": {
                        "amount": 1.0,
                        "unit": "dimensionless",
                        "comment": "water mass/dry mass",
                    },
                    "dry mass": {"amount": 2.0, "unit": "kg"},
                    "price": {"amount": 11.0, "unit": "EUR2005"},
                },
                "activity": None,
                "unit": "kg",
                "amount": 1.0,
                "uncertainty type": 0,
                "loc": 1.0,
            },
            {
                "flow": "075e433b-4be4-448e-9510-9a5029c1ce94",
                "type": "biosphere",
                "name": "Water",
                "classifications": {"CPC": []},
                "production volume": 0.0,
                "properties": {
                    "water in wet mass": {
                        "amount": 1000.0,
                        "unit": "kg",
                        "comment": "water content on a wet matter basis",
                    }
                },
                "unit": "m3",
                "comment": "Calculated value based on literature values and, like, experts, and stuff.",
                "amount": 123456.0,
                "pedigree": {
                    "reliability": 2,
                    "completeness": 2,
                    "temporal correlation": 5,
                    "geographical correlation": 1,
                    "further technological correlation": 1,
                },
                "uncertainty type": 2,
                "loc": 8.0,
                "scale": 2.449489742783178,
                "scale without pedigree": 2.6457513110645907,
            },
        ],
        "filename": "00000_11111111-2222-3333-4444-555555555555_66666666-7777-8888-9999-000000000000.spold",
        "location": "DE",
        "name": "concrete block production",
        "synonyms": [],
        "parameters": {
            "does_it_hurt_when_dropped_on_foot": {
                "description": "How much owwies PLEASE DON'T TELL MOM",
                "id": "daadf2d4-7bbb-4f69-8ab5-58df4c1685eb",
                "unit": "dimensionless",
                "comment": "This is where the people type the words!!!",
                "amount": 7777.0,
                "pedigree": {
                    "reliability": 4,
                    "completeness": 4,
                    "temporal correlation": 3,
                    "geographical correlation": 2,
                    "further technological correlation": 4,
                },
                "uncertainty type": 2,
                "loc": 2.0,
                "scale": 2.0,
                "scale without pedigree": 1.7320508075688772,
            }
        },
        "authors": {
            "data entry": {"name": "Don Ron Bon-Bon", "email": "yummy@example.org"},
            "data generator": {"name": "Rhyme Thyme", "email": "spicy@exaxmple.org"},
        },
        "type": "process",
    }
    assert data[0] == expected


def test_extraction_with_synonyms():
    data = Ecospold2DataExtractor.extract(
        os.path.join(
            FIXTURES,
            "00000_11111111-2222-3333-4444-555555555555_66666666-7777-8888-9999-000000000000_with_synonyms.spold",
        ),
        "ei",
    )
    expected = {
        "comment": "Things and stuff and whatnot\nTechnology:  typical technology for ze Germans!",
        "classifications": [
            ("EcoSpold01Categories", "construction materials/concrete"),
            (
                "ISIC rev.4 ecoinvent",
                "2395:Manufacture of articles of concrete, cement and plaster",
            ),
        ],
        "activity type": "ordinary transforming activity",
        "activity": "c40e3c0a-292f-45a5-88cd-ed18265cb7d7",
        "database": "ei",
        "exchanges": [
            {
                "flow": "6fe4040b-39c7-4d58-b95b-6ee1de4aedb3",
                "type": "technosphere",
                "name": "clay pit infrastructure",
                "classifications": {
                    "CPC": ["53269: Other constructions for manufacturing"]
                },
                "production volume": 0.0,
                "properties": {},
                "activity": "759fac54-b912-4781-9833-0ddd6e8cda24",
                "unit": "unit",
                "comment": "estimated",
                "amount": 9999.0,
                "pedigree": {
                    "reliability": 1,
                    "completeness": 1,
                    "temporal correlation": 5,
                    "geographical correlation": 1,
                    "further technological correlation": 1,
                },
                "uncertainty type": 2,
                "loc": 0.0,
                "scale": 0.4472135954999579,
                "scale without pedigree": 0.31622776601683794,
            },
            {
                "flow": "d4ee8f39-342b-4443-bbb9-c49b6801b5d6",
                "type": "production",
                "name": "concrete block",
                "classifications": {
                    "CPC": ["37510: Non-refractory mortars and concretes"]
                },
                "production volume": 42.0,
                "properties": {
                    "carbon content, non-fossil": {
                        "amount": 0.0,
                        "unit": "dimensionless",
                    },
                    "water content": {
                        "amount": 1.0,
                        "unit": "dimensionless",
                        "comment": "water mass/dry mass",
                    },
                    "dry mass": {"amount": 2.0, "unit": "kg"},
                    "price": {"amount": 11.0, "unit": "EUR2005"},
                },
                "activity": None,
                "unit": "kg",
                "amount": 1.0,
                "uncertainty type": 0,
                "loc": 1.0,
            },
            {
                "flow": "075e433b-4be4-448e-9510-9a5029c1ce94",
                "type": "biosphere",
                "name": "Water",
                "classifications": {"CPC": []},
                "production volume": 0.0,
                "properties": {
                    "water in wet mass": {
                        "amount": 1000.0,
                        "unit": "kg",
                        "comment": "water content on a wet matter basis",
                    }
                },
                "unit": "m3",
                "comment": "Calculated value based on literature values and, like, experts, and stuff.",
                "amount": 123456.0,
                "pedigree": {
                    "reliability": 2,
                    "completeness": 2,
                    "temporal correlation": 5,
                    "geographical correlation": 1,
                    "further technological correlation": 1,
                },
                "uncertainty type": 2,
                "loc": 8.0,
                "scale": 2.449489742783178,
                "scale without pedigree": 2.6457513110645907,
            },
        ],
        "filename": "00000_11111111-2222-3333-4444-555555555555_66666666-7777-8888-9999-000000000000_with_synonyms.spold",
        "location": "DE",
        "name": "concrete block production",
        "synonyms": ["concrete slab production", "concrete block manufacturing"],
        "parameters": {
            "does_it_hurt_when_dropped_on_foot": {
                "description": "How much owwies PLEASE DON'T TELL MOM",
                "id": "daadf2d4-7bbb-4f69-8ab5-58df4c1685eb",
                "unit": "dimensionless",
                "comment": "This is where the people type the words!!!",
                "amount": 7777.0,
                "pedigree": {
                    "reliability": 4,
                    "completeness": 4,
                    "temporal correlation": 3,
                    "geographical correlation": 2,
                    "further technological correlation": 4,
                },
                "uncertainty type": 2,
                "loc": 2.0,
                "scale": 2.0,
                "scale without pedigree": 1.7320508075688772,
            }
        },
        "authors": {
            "data entry": {"name": "Don Ron Bon-Bon", "email": "yummy@example.org"},
            "data generator": {"name": "Rhyme Thyme", "email": "spicy@exaxmple.org"},
        },
        "type": "process",
    }
    print(data[0])
    assert data[0] == expected
