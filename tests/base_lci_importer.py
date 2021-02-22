# -*- coding: utf-8 -*-
from bw2data import Database
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io import ExcelImporter
from bw2io.errors import NonuniqueCode, WrongDatabase
from bw2io.importers.base_lci import LCIImporter
from copy import deepcopy
import numpy as np
import pytest


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
                "formula": "PCB_area * 2",
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
        "parameters": [
            {
                "name": "PCB_mass_total",
                "amount": 0.6,
                "formula": "PCB_cap_mass_film + "
                "PCB_cap_mass_SMD + "
                "PCB_cap_mass_Tantalum",
            }
        ],
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
DATA_NO_PARAMS = [
    {
        "code": "32aa5ab78beda5b8c8efbc89587de7a5",
        "database": "PCB",
        "exchanges": [
            {
                "amount": 100.0,
                "input": ("PCB", "45cb34db4147e510a2561cceec541f6b"),
                "type": "technosphere",
            },
            {
                "amount": 0.0,
                "input": ("PCB", "32aa5ab78beda5b8c8efbc89587de7a5"),
                "type": "production",
            },
        ],
        "location": "CH",
        "name": "mounted printed circuit board",
        "type": "process",
        "unit": "kilogram",
    },
    {
        "code": "45cb34db4147e510a2561cceec541f6b",
        "database": "PCB",
        "exchanges": [
            {
                "amount": 10.0,
                "input": ("PCB", "45cb34db4147e510a2561cceec541f6b"),
                "location": "GLO",
                "type": "production",
            }
        ],
        "location": "CH",
        "name": "unmounted printed circuit board",
        "type": "process",
        "unit": "square meter",
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
@bw2test
def lci():
    obj = LCIImporter("PCB")
    obj.project_parameters = [{"amount": 0.25, "name": "PCB_area"}]
    obj.data = deepcopy(DATA)
    obj.database_parameters = deepcopy(DB)
    return obj


def test_write_database_no_activate(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=False)
    assert [g.name for g in Group.select()] == ["project"]


def test_write_database(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert sorted([g.name for g in Group.select()]) == [
        "PCB",
        "PCB:32aa5ab78beda5b8c8efbc89587de7a5",
        "project",
    ]

    assert ActivityParameter.select().count() == 1
    for x in ActivityParameter.select():
        found = x.dict
    assert found["database"] == "PCB"
    assert found["code"] == "32aa5ab78beda5b8c8efbc89587de7a5"
    assert (
        found["formula"]
        == "PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum"
    )
    assert np.allclose(found["amount"], 0.6)

    given = [
        {
            "database": "PCB",
            "name": "PCB_cap_mass_film",
            "amount": 0.2,
            "maximum": 1.0,
            "minimum": 0.0,
            "uncertainty type": 4.0,
            "unit": "kilogram",
        },
        {
            "database": "PCB",
            "name": "PCB_cap_mass_SMD",
            "amount": 0.2,
            "maximum": 1.0,
            "minimum": 0.0,
            "uncertainty type": 4.0,
            "unit": "kilogram",
        },
        {
            "database": "PCB",
            "name": "PCB_cap_mass_Tantalum",
            "amount": 0.2,
            "maximum": 1.0,
            "minimum": 0.0,
            "uncertainty type": 4.0,
            "unit": "kilogram",
        },
    ]

    assert DatabaseParameter.select().count() == 3
    for x in DatabaseParameter.select():
        assert x.dict in given

    assert ProjectParameter.select().count() == 1
    for x in ProjectParameter.select():
        assert x.dict == {"name": "PCB_area", "amount": 0.25}


def test_no_delete_project_parameters(lci):
    lci.write_project_parameters()
    assert ProjectParameter.select().count()
    d = LCIImporter("PCB")
    assert d.project_parameters is None
    d.write_project_parameters()
    assert ProjectParameter.select().count()

    d.project_parameters = []
    d.write_project_parameters(delete_existing=False)
    assert ProjectParameter.select().count()


def test_delete_project_parameters(lci):
    lci.write_project_parameters()
    assert ProjectParameter.select().count()
    d = LCIImporter("PCB")
    d.project_parameters = []
    d.write_project_parameters()
    assert not ProjectParameter.select().count()


def test_update_project_parameters(lci):
    lci.write_project_parameters()
    assert ProjectParameter.get(name="PCB_area").amount == 0.25
    obj = LCIImporter("PCB")
    obj.project_parameters = [{"amount": 5, "name": "PCB_area"}]
    obj.write_project_parameters()
    assert ProjectParameter.get(name="PCB_area").amount == 5


def test_no_delete_database_parameters(lci):
    assert not DatabaseParameter.select().count()
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert DatabaseParameter.select().count()
    lci.database_parameters = None
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_database(activate_parameters=True)
    assert DatabaseParameter.select().count()


def test_delete_database_parameters(lci):
    assert not DatabaseParameter.select().count()
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert DatabaseParameter.select().count()
    lci.database_parameters = {}
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_database(activate_parameters=True)
    assert not DatabaseParameter.select().count()


def test_update_database_parameters(lci):
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert DatabaseParameter.get(name="PCB_cap_mass_film").amount == 0.2
    lci.database_parameters = [{"amount": 24, "name": "PCB_cap_mass_film"}]
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_database(activate_parameters=True)
    assert DatabaseParameter.get(name="PCB_cap_mass_film").amount == 24


@bw2test
def test_activity_parameters_with_group_name():
    DATA = [
        {
            "code": "A",
            "database": "db",
            "exchanges": [
                {
                    "amount": 0.0,
                    "input": ("db", "A"),
                    "location": "GLO",
                    "name": "mounted printed circuit board",
                    "type": "production",
                    "unit": "kilogram",
                }
            ],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [
                {"name": "something_something", "amount": 0.8, "group": "g"}
            ],
            "type": "process",
            "unit": "kilogram",
        }
    ]
    obj = LCIImporter("db")
    obj.data = deepcopy(DATA)
    obj.write_database(activate_parameters=True)
    assert Group.select().where(Group.name == "g").count() == 1
    assert Group.select().count() == 1
    assert ActivityParameter.get(name="something_something").amount == 0.8


@bw2test
def test_activity_multiple_activities_same_group_name():
    DATA = [
        {
            "code": "A",
            "database": "db",
            "exchanges": [
                {
                    "amount": 0.0,
                    "input": ("db", "A"),
                    "location": "GLO",
                    "type": "production",
                }
            ],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [
                {"name": "something_something", "amount": 0.8, "group": "g"}
            ],
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "B",
            "database": "db",
            "exchanges": [
                {
                    "amount": 0.0,
                    "input": ("db", "B"),
                    "location": "GLO",
                    "type": "production",
                }
            ],
            "location": "GLO",
            "name": "bla bla",
            "parameters": [{"name": "something_else", "amount": 0.2, "group": "g"}],
            "type": "process",
            "unit": "kilogram",
        },
    ]
    obj = LCIImporter("db")
    obj.data = deepcopy(DATA)
    obj.write_database(activate_parameters=True)
    assert Group.select().where(Group.name == "g").count() == 1
    assert Group.select().count() == 1
    assert ActivityParameter.get(name="something_something").amount == 0.8
    assert ActivityParameter.get(name="something_else").amount == 0.2


@bw2test
def test_wrongdatabase_error_code():
    lci = LCIImporter("woo")
    data = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "woo",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [],
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "45cb34db4147e510a2561cceec541f6b",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "parameters": [],
            "name": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
        },
    ]
    with pytest.raises(WrongDatabase):
        lci.write_database(data)


@bw2test
def test_nonuniquecode_error_code():
    lci = LCIImporter("woo")
    data = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "woo",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [],
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "woo",
            "exchanges": [],
            "location": "GLO",
            "parameters": [],
            "name": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
        },
    ]
    with pytest.raises(NonuniqueCode):
        lci.write_database(data)


def test_database_update_existing_data(lci):
    lci.write_database()
    assert sum(exc["amount"] for act in Database("PCB") for exc in act.exchanges()) == 1
    assert {act["location"] for act in Database("PCB")} == {"GLO"}
    lci.data = deepcopy(DATA_NO_PARAMS)
    lci.write_database()
    assert (
        sum(exc["amount"] for act in Database("PCB") for exc in act.exchanges()) == 110
    )
    assert {act["location"] for act in Database("PCB")} == {"CH"}


def test_update_activity_parameters(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert (
        ActivityParameter.get(name="PCB_mass_total").formula
        == "PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum"
    )
    new = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [
                {
                    "name": "PCB_mass_total",
                    "amount": 0.6,
                    "formula": "PCB_cap_mass_film + 2",
                }
            ],
            "type": "process",
            "unit": "kilogram",
        },
    ]
    obj = LCIImporter("PCB")
    obj.data = deepcopy(new)
    obj.write_database(activate_parameters=True)
    assert (
        ActivityParameter.get(name="PCB_mass_total").formula == "PCB_cap_mass_film + 2"
    )


def test_activity_parameters_delete_old_groupname(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert (
        ActivityParameter.select()
        .where(ActivityParameter.group == "PCB:32aa5ab78beda5b8c8efbc89587de7a5")
        .count()
    )
    assert (
        not ActivityParameter.select()
        .where(ActivityParameter.group == "some other group")
        .count()
    )
    new = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [
                {
                    "name": "PCB_mass_total",
                    "amount": 0.6,
                    "group": "some other group",
                    "formula": "PCB_cap_mass_film + 2",
                }
            ],
            "type": "process",
            "unit": "kilogram",
        },
    ]
    obj = LCIImporter("PCB")
    obj.data = deepcopy(new)
    obj.write_database(activate_parameters=True)
    assert (
        not ActivityParameter.select()
        .where(ActivityParameter.group == "PCB:32aa5ab78beda5b8c8efbc89587de7a5")
        .count()
    )


def test_delete_activity_parameters_delete_existing(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    new = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [],
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "45cb34db4147e510a2561cceec541f6b",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "parameters": [],
            "name": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
        },
    ]
    obj = LCIImporter("PCB")
    obj.data = new
    obj.write_database(activate_parameters=False, delete_existing=True)
    assert ActivityParameter.select().count()
    obj = LCIImporter("PCB")
    obj.data = new
    obj.write_database(activate_parameters=True, delete_existing=True)
    assert not ActivityParameter.select().count()


def test_no_delete_pe_no_activate_parameters(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert ParameterizedExchange.select().count()
    new = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": {},
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "45cb34db4147e510a2561cceec541f6b",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "parameters": {},
            "name": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
        },
    ]
    obj = LCIImporter("PCB")
    obj.data = new
    obj.write_database(activate_parameters=False, delete_existing=True)
    assert ParameterizedExchange.select().count()


def test_delete_pe_delete_existing(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    assert ParameterizedExchange.select().count()
    new = [
        {
            "code": "32aa5ab78beda5b8c8efbc89587de7a5",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": {},
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "45cb34db4147e510a2561cceec541f6b",
            "database": "PCB",
            "exchanges": [],
            "location": "GLO",
            "parameters": {},
            "name": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
        },
    ]
    obj = LCIImporter("PCB")
    obj.data = new
    obj.write_database(activate_parameters=True, delete_existing=True)
    assert not ParameterizedExchange.select().count()


@bw2test
def test_delete_pe_update_still_deletes():
    DATA = [
        {
            "code": "A",
            "database": "db",
            "exchanges": [
                {
                    "amount": 0.0,
                    "input": ("db", "A"),
                    "location": "GLO",
                    "type": "production",
                    "formula": "3 + 4",
                }
            ],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": [
                {"name": "something_something", "amount": 0.8, "group": "g"}
            ],
            "type": "process",
            "unit": "kilogram",
        },
        {
            "code": "B",
            "database": "db",
            "exchanges": [
                {
                    "amount": 0.0,
                    "input": ("db", "B"),
                    "location": "GLO",
                    "type": "production",
                    "formula": "1 + 2",
                }
            ],
            "location": "GLO",
            "name": "bla bla",
            "parameters": [{"name": "something_else", "amount": 0.2, "group": "h"}],
            "type": "process",
            "unit": "kilogram",
        },
    ]
    obj = LCIImporter("db")
    obj.data = DATA
    obj.write_database(activate_parameters=True)
    assert Group.select().where(Group.name == "g").count() == 1
    assert Group.select().where(Group.name == "h").count() == 1
    assert Group.select().count() == 2
    assert ParameterizedExchange.get(group="g").formula == "3 + 4"
    assert ParameterizedExchange.get(group="h").formula == "1 + 2"

    new = [
        {
            "code": "B",
            "database": "db",
            "exchanges": [
                {
                    "amount": 0.0,
                    "input": ("db", "B"),
                    "location": "GLO",
                    "type": "production",
                    "formula": "6 + 7",
                }
            ],
            "location": "GLO",
            "name": "bla bla",
            "parameters": [{"name": "something_else", "amount": 0.2, "group": "h"}],
            "type": "process",
            "unit": "kilogram",
        }
    ]
    obj = LCIImporter("db")
    obj.data = new
    obj.write_database(delete_existing=False, activate_parameters=True)
    assert (
        not ParameterizedExchange.select()
        .where(ParameterizedExchange.group == "g")
        .count()
    )
    assert (
        ParameterizedExchange.select().where(ParameterizedExchange.group == "h").count()
        == 1
    )
    assert ParameterizedExchange.get(group="h").formula == "6 + 7"
