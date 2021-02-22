# -*- coding: utf-8 -*-
from bw2data import Database, get_activity
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io import ExcelImporter
from copy import deepcopy
import os
import pytest

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


DATA = [
    {
        "arbitrary": "metadata",
        "code": "32aa5ab78beda5b8c8efbc89587de7a5",
        "comment": "something important here maybe?",
        "database": "PCB",
        "exchanges": [
            {
                "amount": 0.0,
                "input": ("PCB", "45cb34db4147e510a2561cceec541f6b"),
                "formula": "3 + 4",
                "location": "GLO",
                "name": "unmounted printed circuit board",
                "type": "technosphere",
                "unit": "square meter",
            },
            {
                "amount": 0.0,
                "input": ("PCB", "32aa5ab78beda5b8c8efbc89587de7a5"),
                "formula": "PCB_mass_total",
                "location": "GLO",
                "name": "mounted printed circuit board",
                "type": "production",
                "unit": "kilogram",
            },
        ],
        "location": "GLO",
        "name": "mounted printed circuit board",
        "parameters": [{"name": "PCB_mass_total", "amount": 0.6, "formula": "1 + 2"}],
        "production amount": 0.0,
        "reference product": "mounted printed circuit board",
        "type": "process",
        "unit": "kilogram",
        "worksheet name": "PCB inventory",
    },
    {
        "categories": ("electronics", "board"),
        "code": "45cb34db4147e510a2561cceec541f6b",
        "comment": "one input",
        "database": "PCB",
        "exchanges": [
            {
                "amount": 1.0,
                "input": ("PCB", "45cb34db4147e510a2561cceec541f6b"),
                "location": "GLO",
                "name": "unmounted printed circuit board",
                "type": "production",
                "uncertainty type": 0,
                "unit": "square meter",
            }
        ],
        "location": "GLO",
        "name": "unmounted printed circuit board",
        "production amount": 1.0,
        "reference product": "unmounted printed circuit board",
        "type": "process",
        "unit": "square meter",
        "worksheet name": "PCB inventory",
    },
]
DB = [
    {
        "amount": 0.2,
        "maximum": 1.0,
        "minimum": 0.0,
        "name": "PCB_cap_mass_film",
        "uncertainty type": 4.0,
        "unit": "kilogram",
    },
    {
        "amount": 0.2,
        "maximum": 1.0,
        "minimum": 0.0,
        "name": "PCB_cap_mass_SMD",
        "uncertainty type": 4.0,
        "unit": "kilogram",
    },
    {
        "amount": 0.2,
        "maximum": 1.0,
        "minimum": 0.0,
        "name": "PCB_cap_mass_Tantalum",
        "uncertainty type": 4.0,
        "unit": "kilogram",
    },
]


@pytest.fixture
def no_init(monkeypatch):
    monkeypatch.setattr("bw2io.importers.excel.ExcelImporter.__init__", lambda x: None)


@bw2test
def test_write_only_database_parameters(no_init):
    Database("foo").register()
    obj = ExcelImporter()
    obj.db_name = "foo"
    obj.data = None
    obj.database_parameters = deepcopy(DB)
    obj.write_database_parameters(activate_parameters=False)
    assert not DatabaseParameter.select().count()
    assert "parameters" in obj.metadata

    obj.write_database_parameters()
    assert DatabaseParameter.select().count() == 3

    obj.database_parameters = deepcopy(DB[:1])
    obj.write_database_parameters()
    assert DatabaseParameter.select().count() == 1


@bw2test
def test_write_only_activity_parameters(no_init):
    assert not ActivityParameter.select().count()
    Database("PCB").register()
    obj = ExcelImporter()
    obj.db_name = "PCB"
    obj.data = deepcopy(DATA)
    obj.write_database()
    assert ActivityParameter.select().count() == 1
    assert ActivityParameter.get().amount != 7

    NEW = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "PCB",
            "parameters": [{"name": "PCB_mass_total", "amount": 11, "formula": "7"}],
        }
    ]
    obj.write_activity_parameters(NEW)
    assert ActivityParameter.select().count() == 1
    a = ActivityParameter.get()
    assert a.formula == "7"
    assert a.amount == 7


@bw2test
def test_write_only_activity_parameters_no_activate_others(no_init):
    Database("PCB").register()
    obj = ExcelImporter()
    obj.db_name = "PCB"
    obj.data = deepcopy(DATA)
    obj.write_database(activate_parameters=False)

    NEW = [
        {
            "code": "45cb34db4147e510a2561cceec541f6b",
            "database": "PCB",
            "exchanges": [],
            "name": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
            "parameters": [{"name": "something_test", "amount": 2, "formula": "3 + 2"}],
        }
    ]
    obj = ExcelImporter()
    obj.db_name = "PCB"
    obj.write_activity_parameters(NEW)

    assert ActivityParameter.select().count() == 1
    assert ActivityParameter.get().formula == "3 + 2"
    assert "parameters" not in get_activity(("PCB", "45cb34db4147e510a2561cceec541f6b"))
    assert "parameters" in get_activity(("PCB", "32aa5ab78beda5b8c8efbc89587de7a5"))


@bw2test
def test_empty_activity_parameters_dont_delete(no_init):
    # Empty activity parameters aren't considered at all
    # so they shouldn't be deleted if empty section provided
    Database("PCB").register()
    obj = ExcelImporter()
    obj.db_name = "PCB"
    obj.data = deepcopy(DATA)
    obj.write_database()
    assert len(parameters)

    NEW = deepcopy(DATA)
    for ds in NEW:
        if "parameters" in ds:
            del ds["parameters"]

    Database("PCB").register()
    obj = ExcelImporter()
    obj.db_name = "PCB"
    obj.data = NEW
    obj.write_database(delete_existing=False)
    assert len(parameters)


@bw2test
def test_no_valid_worksheets():
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "empty.xlsx"))
    for attr in ("db_name", "data"):
        assert not hasattr(ei, attr)


@bw2test
def test_no_valid_worksheets_all_columns_cutoff():
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "basic_all_cutoff.xlsx"))
    assert not ei.data
