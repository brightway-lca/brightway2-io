from copy import deepcopy

import pytest
from bw2data import Database
from bw2data.parameters import *
from bw2data.tests import bw2test

from bw2io import ExcelImporter


@pytest.fixture
def no_init(monkeypatch):
    monkeypatch.setattr("bw2io.importers.excel.ExcelImporter.__init__", lambda x: None)


@bw2test
def test_get_labelled_section(no_init):
    ei = ExcelImporter()
    with pytest.raises(AssertionError):
        assert ei.get_labelled_section(None, [["", ""]])
    with pytest.raises(AssertionError):
        assert ei.get_labelled_section(None, [["foo", "", "bar"]])

    data = [
        ["Parameters", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["name", "amount", "formula", "", "", "", "", "", "", "", "", "", "", ""],
        [
            "something::something",
            0.6,
            "A + B",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["Nope", 1.3, "", "Will be skipped", "", "", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
    ]
    expected = [
        {"name": "something::something", "amount": 0.6, "formula": "A + B"},
        {"name": "Nope", "amount": 1.3},
    ]
    assert ei.get_labelled_section(None, data, 1, transform=False) == expected
    expected = [
        {"name": ("something", "something"), "amount": 0.6, "formula": "A + B"},
        {"name": "Nope", "amount": 1.3},
    ]
    assert ei.get_labelled_section(None, data, 1) == expected
    given = [
        ["name", "amount", "formula", "", "", "", "", "", "", "", "", "", "", ""],
        [
            "PCB_mass_total",
            0.6,
            "PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    expected = [
        {
            "name": "PCB_mass_total",
            "amount": 0.6,
            "formula": "PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum",
        }
    ]
    assert ei.get_labelled_section(None, given) == expected


@bw2test
def test_process_activities(no_init, monkeypatch):
    monkeypatch.setattr(
        "bw2io.importers.excel.ExcelImporter.get_activity", lambda a, b, c: c
    )
    ei = ExcelImporter()
    with pytest.raises(ValueError):
        assert ei.process_activities([("name", [["cutoff", "foo"]])])

    given = [
        ("n", [["cutoff", "2"], ["", ""], ["activity", "foo", "bar"], ["1", "2", "3"]])
    ]
    expected = [[["activity", "foo"], ["1", "2"]]]
    assert ei.process_activities(given) == expected


@bw2test
def test_get_activity(no_init):
    given = [
        [
            "Activity",
            "mounted printed circuit board",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        [
            "comment",
            "something important here maybe?",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["arbitrary", "metadata", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["location", "GLO", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["type", "process", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["unit", "kilogram", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["Parameters", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["name", "amount", "formula", "", "", "", "", "", "", "", "", "", "", ""],
        [
            "PCB_mass_total",
            0.6,
            "PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["Exchanges", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        [
            "name",
            "amount",
            "unit",
            "database",
            "location",
            "type",
            "formula",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        [
            "unmounted printed circuit board",
            0.0,
            "square meter",
            "PCB",
            "GLO",
            "technosphere",
            "PCB_area * 2",
            "PCB_area * 2",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        [
            "mounted printed circuit board",
            0.0,
            "kilogram",
            "PCB",
            "GLO",
            "production",
            "PCB_mass_total",
            "PCB_mass_total",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        [
            "Activity",
            "unmounted printed circuit board",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    ei = ExcelImporter()
    ei.db_name = "db"
    expected = {
        "arbitrary": "metadata",
        "comment": "something important here maybe?",
        "database": "db",
        "exchanges": [
            {
                "database": "PCB",
                "amount": 0.0,
                "formula": "PCB_area * 2",
                "location": "GLO",
                "name": "unmounted printed circuit board",
                "type": "technosphere",
                "unit": "square meter",
            },
            {
                "database": "PCB",
                "amount": 0.0,
                "formula": "PCB_mass_total",
                "location": "GLO",
                "name": "mounted printed circuit board",
                "type": "production",
                "unit": "kilogram",
            },
        ],
        "location": "GLO",
        "name": "mounted printed circuit board",
        "parameters": {
            "PCB_mass_total": {
                "amount": 0.6,
                "formula": "PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum",
            }
        },
        "type": "process",
        "unit": "kilogram",
        "worksheet name": "a",
    }
    assert ei.get_activity("a", given) == expected


@bw2test
def test_get_activity_metadata_indexing(no_init):
    given = [
        [
            "Activity",
            "mounted printed circuit board",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["arbitrary", "metadata", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["unit", "kilogram", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["Parameters", "", "", "", "", "", "", "", "", "", "", "", "", ""],
    ]
    ei = ExcelImporter()
    ei.db_name = "db"
    expected = {
        "arbitrary": "metadata",
        "database": "db",
        "name": "mounted printed circuit board",
        "exchanges": [],
        "unit": "kilogram",
        "worksheet name": "a",
    }
    assert ei.get_activity("a", given) == expected

    given = [
        [
            "Activity",
            "mounted printed circuit board",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["arbitrary", "metadata", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["unit", "kilogram", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],
    ]
    ei = ExcelImporter()
    ei.db_name = "db"
    expected = {
        "arbitrary": "metadata",
        "database": "db",
        "name": "mounted printed circuit board",
        "exchanges": [],
        "unit": "kilogram",
        "worksheet name": "a",
    }
    assert ei.get_activity("a", given) == expected

    given = [
        [
            "Activity",
            "mounted printed circuit board",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        ["arbitrary", "metadata", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["unit", "kilogram", "", "", "", "", "", "", "", "", "", "", "", ""],
    ]
    ei = ExcelImporter()
    ei.db_name = "db"
    expected = {
        "arbitrary": "metadata",
        "database": "db",
        "name": "mounted printed circuit board",
        "exchanges": [],
        "unit": "kilogram",
        "worksheet name": "a",
    }
    assert ei.get_activity("a", given) == expected
