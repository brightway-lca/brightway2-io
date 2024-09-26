import os

import pytest

from bw2data.tests import bw2test
from bw2data.database import DatabaseChooser
from bw2io import BW2Package


FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "bw2package")


@pytest.fixture
def mini_biosphere():
    return {
        ("biosphere3", "9ec076d9-6d9f-4a0b-9851-730626ed4319"): {
            "categories": ("air",),
            "code": "9ec076d9-6d9f-4a0b-9851-730626ed4319",
            "CAS number": "007782-44-7",
            "synonyms": ["molecular oxygen"],
            "name": "Oxygen",
            "database": "biosphere3",
            "unit": "kilogram",
            "type": "emission",
            },
        ("biosphere3", "14ea575b-5caa-4958-acf7-0bcc47f9cadf"): {
            "categories": ("soil",),
            "code": "14ea575b-5caa-4958-acf7-0bcc47f9cadf",
            "CAS number": "007440-44-0",
            "synonyms": [],
            "name": "Carbon",
            "database": "biosphere3",
            "unit": "kilogram",
            "type": "emission",
            },
        ("biosphere3", "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7"): {
            "categories": ("air",),
            "code": "eba59fd6-f37e-41dc-9ca3-c7ea22d602c7",
            "CAS number": "000124-38-9",
            "synonyms": ["Carbon dioxide"],
            "name": "Carbon dioxide, non-fossil",
            "database": "biosphere3",
            "unit": "kilogram",
            "type": "emission",
            },
        }


@bw2test
def test_bw2_compat(mini_biosphere):
    mini_biosphere3_db = DatabaseChooser("biosphere3")
    mini_biosphere3_db.register()
    mini_biosphere3_db.write(mini_biosphere)

    obj = BW2Package.import_file(os.path.join(FIXTURES, "bw2_compat_test.bw2package"))[0]

    a = obj.get("7599062216496486961")
    assert a["name"] == "partial_respiration"
    assert a["unit"] == "g"
    assert a["type"] == "process"

    a = obj.get("3866902554231372371")
    assert a["name"] == "C_inactivated"
    assert a["unit"] == "g"
    assert a["type"] == "production"
