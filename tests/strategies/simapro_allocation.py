from bw2io.strategies.simapro import sp_allocate_functional_products


def test_sp_allocate_functional_products():
    given = [
        {
            "type": "multifunctional",
            "exchanges": [
                {
                    "name": "Biowaste",
                    "unit": "kg",
                    "comment": "Manure",
                    "amount": 10,
                    "uncertainty type": 0,
                    "loc": 10,
                    "type": "production",
                    "functional": False,
                },
                {
                    "name": "Quack",
                    "unit": "p",
                    "amount": 5,
                    "uncertainty type": 0,
                    "loc": 5,
                    "type": "technosphere",
                },
                {
                    "name": "Wool",
                    "unit": "kg",
                    "amount": 1000.0,
                    "allocation": 3,
                    "type": "production",
                    "functional": True,
                },
                {
                    "name": "Strips",
                    "unit": "kg",
                    "amount": 2000.0,
                    "allocation": 2,
                    "type": "technosphere",
                    "functional": True,
                },
                {
                    "name": "Viscera",
                    "unit": "kg",
                    "amount": 3000.0,
                    "allocation": 0,
                    "type": "production",
                    "functional": True,
                },
            ],
        }
    ]
    expected = [
        {
            "type": "multifunctional",
            "exchanges": [
                {
                    "name": "Biowaste",
                    "unit": "kg",
                    "comment": "Manure",
                    "amount": 10,
                    "uncertainty type": 0,
                    "loc": 10,
                    "type": "production",
                    "functional": False,
                },
                {
                    "name": "Quack",
                    "unit": "p",
                    "amount": 5,
                    "uncertainty type": 0,
                    "loc": 5,
                    "type": "technosphere",
                },
                {
                    "name": "Wool",
                    "unit": "kg",
                    "amount": 1000.0,
                    "allocation": 3,
                    "type": "production",
                    "functional": True,
                },
                {
                    "name": "Strips",
                    "unit": "kg",
                    "amount": 2000.0,
                    "allocation": 2,
                    "type": "technosphere",
                    "functional": True,
                },
                {
                    "name": "Viscera",
                    "unit": "kg",
                    "amount": 3000.0,
                    "allocation": 0,
                    "type": "production",
                    "functional": True,
                },
            ],
        },
        {
            "type": "process",
            "exchanges": [
                {
                    "name": "Wool",
                    "unit": "kg",
                    "amount": 1000.0,
                    "type": "production",
                    "functional": True,
                },
                {
                    "name": "Biowaste",
                    "unit": "kg",
                    "comment": "Manure",
                    "amount": 6.0,
                    "uncertainty type": 0,
                    "loc": 6.0,
                    "type": "production",
                    "functional": False,
                },
                {
                    "name": "Quack",
                    "unit": "p",
                    "amount": 3.0,
                    "uncertainty type": 0,
                    "loc": 3.0,
                    "type": "technosphere",
                },
            ],
            "name": "Wool",
            "reference product": "Wool",
            "unit": "kg",
            "production amount": 1000.0,
        },
        {
            "type": "process",
            "exchanges": [
                {
                    "name": "Strips",
                    "unit": "kg",
                    "amount": 2000.0,
                    "type": "technosphere",
                    "functional": True,
                },
                {
                    "name": "Biowaste",
                    "unit": "kg",
                    "comment": "Manure",
                    "amount": 4.0,
                    "uncertainty type": 0,
                    "loc": 4.0,
                    "type": "production",
                    "functional": False,
                },
                {
                    "name": "Quack",
                    "unit": "p",
                    "amount": 2.0,
                    "uncertainty type": 0,
                    "loc": 2.0,
                    "type": "technosphere",
                },
            ],
            "name": "Strips",
            "reference product": "Strips",
            "unit": "kg",
            "production amount": 2000.0,
        },
    ]
    assert sp_allocate_functional_products(given) == expected
