import copy

import pytest
from bw2data import Database, databases
from bw2data.tests import bw2test

from bw2io.strategies.simapro import flip_sign_on_waste


@bw2test
def test_waste_sign_changed():
    assert not len(Database("other"))
    other = Database("other")
    other.register()
    other.write(
        {
            ("other", "non-waste"): {
                "name": "production activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "non-waste product",
                "production amount": 1,  # Positive, not a waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "non-waste product",
                        "unit": "kilogram",
                        "amount": 1.0,
                        "input": ("other", "non-waste"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
            ("other", "waste-0"): {
                "name": "waste treatment activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "waste product",
                "production amount": -1,  # negative, waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "waste treatment",
                        "unit": "kilogram",
                        "amount": -1.0,
                        "input": ("other", "waste-0"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
            ("other", "waste-1"): {
                "name": "waste treatment activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "waste product",
                "production amount": -1,  # negative, waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "waste treatment",
                        "unit": "kilogram",
                        "amount": -1.0,
                        "input": ("other", "waste-1"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
            ("other", "waste-2"): {
                "name": "waste treatment activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "waste product",
                "production amount": -1,  # negative, waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "waste treatment",
                        "unit": "kilogram",
                        "amount": -1.0,
                        "input": ("other", "waste-2"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
            ("other", "waste-3"): {
                "name": "waste treatment activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "waste product",
                "production amount": -1,  # negative, waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "waste treatment",
                        "unit": "kilogram",
                        "amount": -1.0,
                        "input": ("other", "waste-3"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
            ("other", "waste-4"): {
                "name": "waste treatment activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "waste product",
                "production amount": -1,  # negative, waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "waste treatment",
                        "unit": "kilogram",
                        "amount": -1.0,
                        "input": ("other", "waste-4"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
            ("other", "waste-5"): {
                "name": "waste treatment activity",
                "unit": "kilogram",
                "location": "GLO",
                "reference product": "waste product",
                "production amount": -1,  # negative, waste treatment
                "activity type": "ordinary transforming activity",
                "exchanges": [
                    {
                        "name": "waste treatment",
                        "unit": "kilogram",
                        "amount": -1.0,
                        "input": ("other", "waste-5"),
                        "type": "production",
                        "uncertainty type": 0,
                    },
                ],
            },
        }
    )
    assert "other" in databases
    db = [
        {
            "simapro metadata": dict(),
            "code": "test_non_waste",
            "database": "sp",
            "type": "process",
            "name": "test_non_waste",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything",
            "production amount": 1,
            "exchanges": [
                {
                    "name": "test_non_waste",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_non_waste"),
                    "type": "production",
                    "uncertainty type": 0,
                },
                {
                    "name": "some product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "non-waste"),
                    "type": "technosphere",
                    "uncertainty type": 0,
                },
            ],
        },
        {
            "simapro metadata": dict(),
            "code": "test_waste_0",
            "database": "sp",
            "type": "process",
            "name": "test_waste_0",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything else",
            "production amount": 1,
            "exchanges": [
                {
                    "name": "test_waste_0",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_waste_0"),
                    "type": "production",
                    "uncertainty type": 0,
                },
                {
                    "name": "waste product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "waste-0"),
                    "type": "technosphere",
                    "uncertainty type": 0,
                },
            ],
        },
        {
            "simapro metadata": dict(),
            "code": "test_waste_1",
            "database": "sp",
            "type": "process",
            "name": "test_waste_1",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything else",
            "production amount": 1,
            "exchanges": [
                {
                    "name": "test_waste_1",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_waste_1"),
                    "type": "production",
                    "uncertainty type": 0,
                },
                {
                    "name": "waste product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "waste-1"),
                    "type": "technosphere",
                    "uncertainty type": 1,
                    "loc": 1.0,
                },
            ],
        },
        {
            "simapro metadata": dict(),
            "code": "test_waste_2",
            "database": "sp",
            "type": "process",
            "name": "test_waste_2",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything else",
            "production amount": 1,
            "activity type": "ordinary transforming activity",
            "exchanges": [
                {
                    "name": "test_waste_2",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_waste_2"),
                    "type": "production",
                },
                {
                    "name": "waste product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "waste-0"),
                    "type": "technosphere",
                    "uncertainty type": 2,
                    "loc": 0,
                    "scale": 0.1,
                },
            ],
        },
        {
            "simapro metadata": dict(),
            "code": "test_waste_3",
            "database": "sp",
            "type": "process",
            "name": "test_waste_3",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything else",
            "production amount": 1,
            "activity type": "ordinary transforming activity",
            "exchanges": [
                {
                    "name": "test_waste_3",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_waste_3"),
                    "type": "production",
                    "uncertainty type": 0,
                },
                {
                    "name": "waste product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "waste-3"),
                    "type": "technosphere",
                    "uncertainty type": 3,
                    "loc": 1.0,
                    "scale": 0.1,
                },
            ],
        },
        {
            "simapro metadata": dict(),
            "code": "test_waste_4",
            "database": "sp",
            "type": "process",
            "name": "test_waste_4",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything else",
            "production amount": 1,
            "activity type": "ordinary transforming activity",
            "exchanges": [
                {
                    "name": "test_waste_4",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_waste_4"),
                    "type": "production",
                    "uncertainty type": 0,
                },
                {
                    "name": "waste product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "waste-4"),
                    "type": "technosphere",
                    "uncertainty type": 4,
                    "minimum": 0.5,
                    "maximum": 1.5,
                },
            ],
        },
        {
            "simapro metadata": dict(),
            "code": "test_waste_5",
            "database": "sp",
            "type": "process",
            "name": "test_waste_5",
            "unit": "kilogram",
            "location": "GLO",
            "reference product": "anything else",
            "production amount": 1,
            "activity type": "ordinary transforming activity",
            "exchanges": [
                {
                    "name": "test_waste_5",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("sp", "test_waste_5"),
                    "type": "production",
                    "uncertainty type": 0,
                },
                {
                    "name": "waste product",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "input": ("other", "waste-5"),
                    "type": "technosphere",
                    "uncertainty type": 5,
                    "minimum": 0.5,
                    "maximum": 1.5,
                    "loc": 1.0,
                },
            ],
        },
    ]

    db_before = copy.deepcopy(db)
    db = flip_sign_on_waste(db, "other")
    # Check that things did not unexpectedly change
    expected_unchanged_keys_act = [
        "simapro metadata",
        "code",
        "database",
        "name",
        "unit",
        "location",
        "reference product",
        "type",
        "production amount",
    ]

    expected_unchanged_keys_exc = [
        "name",
        "unit",
        "input",
        "type",
        "uncertainty type",
    ]
    for old_act, new_act in zip(db_before, db):
        for act_k in expected_unchanged_keys_act:
            assert old_act[act_k] == new_act[act_k]
            for old_exc, new_exc in zip(old_act["exchanges"], new_act["exchanges"]):
                for exc_k in expected_unchanged_keys_exc:
                    assert old_exc.get(exc_k, "key not found") == new_exc.get(
                        exc_k, "key not found"
                    )
    # Check that inputs of regular products have not changed
    assert db[0] == db_before[0]
    # Check uncertainty types 0 (undefined)
    for new_exc, old_exc in zip(db[1]["exchanges"], db_before[1]["exchanges"]):
        if new_exc["type"] == "production":
            assert new_exc == old_exc
        else:
            assert new_exc["amount"] == -1

    # Check uncertainty types 1 (no uncertainty)
    for new_exc, old_exc in zip(db[2]["exchanges"], db_before[2]["exchanges"]):
        if new_exc["type"] == "production":
            assert new_exc == old_exc
        else:
            assert new_exc["amount"] == -1

    # Check uncertainty type 2 (lognormal)
    for new_exc, old_exc in zip(db[3]["exchanges"], db_before[3]["exchanges"]):
        if new_exc["type"] == "production":
            assert new_exc == old_exc
        else:
            assert new_exc["amount"] == -1
            assert new_exc["loc"] == 0  # ln(1)
            assert new_exc["scale"] == old_exc["scale"]  # no change
            assert new_exc["negative"] == True

    # Check uncertainty type 3 (normal)
    for new_exc, old_exc in zip(db[4]["exchanges"], db_before[4]["exchanges"]):
        if new_exc["type"] == "production":
            assert new_exc == old_exc
        else:
            assert new_exc["amount"] == -1
            assert new_exc["loc"] == -1
            assert new_exc["scale"] == old_exc["scale"]  # no change

    # Check uncertainty type 4 (uniform)
    for new_exc, old_exc in zip(db[5]["exchanges"], db_before[5]["exchanges"]):
        if new_exc["type"] == "production":
            assert new_exc == old_exc
        else:
            assert new_exc["amount"] == -1
            assert new_exc["minimum"] == -1.5
            assert new_exc["maximum"] == -0.5

    # Check uncertainty type 5 (triangular)
    for new_exc, old_exc in zip(db[6]["exchanges"], db_before[6]["exchanges"]):
        if new_exc["type"] == "production":
            assert new_exc == old_exc
        else:
            assert new_exc["amount"] == -1
            assert new_exc["loc"] == -1
            assert new_exc["minimum"] == -1.5
            assert new_exc["maximum"] == -0.5
