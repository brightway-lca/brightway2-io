# -*- coding: utf-8 -*-
from bw2data import Database
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io.export.csv import CSVFormatter, write_lci_csv
from bw2io.export.excel import write_lci_excel
from bw2io.extractors.csv import CSVExtractor
from bw2io.extractors.excel import ExcelExtractor
from bw2io.importers.excel import ExcelImporter
import os
import pytest

CSV_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "csv")
EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


@pytest.fixture
@bw2test
def setup():
    db = Database("example")
    db.register(extra="yes please")

    a = db.new_activity(code="A", name="An activity", unit="kg", foo="bar")
    a.save()
    a.new_exchange(amount=1, input=a, type="production").save()

    b = db.new_activity(code="B", name="Another activity", location="here", this="that")
    b.save()
    b.new_exchange(amount=10, input=b, type="production").save()
    a.new_exchange(
        amount=0, input=b, type="technosphere", formula="foo * bar + 4"
    ).save()

    project_data = [
        {"name": "foo", "formula": "green / 7",},
        {"name": "green", "amount": 7},
    ]
    parameters.new_project_parameters(project_data)

    database_data = [
        {"name": "red", "formula": "(foo + blue ** 2) / 5",},
        {"name": "blue", "amount": 12},
    ]
    parameters.new_database_parameters(database_data, "example")

    activity_data = [
        {
            "name": "reference_me",
            "formula": "sqrt(red - 20)",
            "database": "example",
            "code": "B",
        },
        {
            "name": "bar",
            "formula": "reference_me + 2",
            "database": "example",
            "code": "A",
        },
    ]
    parameters.new_activity_parameters(activity_data, "my group")

    parameters.add_exchanges_to_group("my group", a)
    ActivityParameter.recalculate_exchanges("my group")


def test_write_lci_csv_complicated(setup):
    given = CSVExtractor.extract(write_lci_csv("example"))[1]
    expected = CSVExtractor.extract(os.path.join(CSV_FIXTURES_DIR, "complicated.csv"))[
        1
    ]
    assert given == expected


def test_write_lci_excel_complicated(setup):
    fp = write_lci_excel("example")
    given = ExcelExtractor.extract(fp)[0][1]
    expected = ExcelExtractor.extract(
        os.path.join(EXCEL_FIXTURES_DIR, "export-complicated.xlsx")
    )[0][1]
    assert given == expected


@bw2test
def test_write_lci_excel_rich_data_skipped():
    Database("foo").write({
        ("foo", "a"): {
            'this': {"should": "be skipped"},
            "name": "bar",
            "exchanges": []
        }
    })
    fp = write_lci_excel("foo")
    given = ExcelExtractor.extract(fp)[0][1]
    expected = [['Database', 'foo'], [None, None], ['Activity', 'bar'], ['code', 'a'], ['Exchanges', None]]
    assert given == expected


def test_roundtrip_excel_complicated(setup):
    pass


def test_excel_roundtrip_update(setup):
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "complicated-modified.xlsx"))
    # TODO


def test_write_lci_sections(setup):
    expected = [
        ["Database", "example"],
        ["extra", "yes please"],
        [],
        ["Database parameters"],
        ["name", "amount", "formula", "database"],
        ["blue", "12.0", "", "example"],
        ["red", "29.0", "(foo + blue ** 2) / 5", "example"],
        [],
    ]
    given = CSVExtractor.extract(
        write_lci_csv("example", sections=["database", "database parameters"])
    )[1]
    assert given == expected

    expected = [
        ["Activity", "An activity"],
        ["code", "A"],
        ["foo", "bar"],
        ["location", "GLO"],
        ["unit", "kg"],
        ["Exchanges"],
        ["name", "amount", "location", "unit", "type", "formula", "original_amount"],
        ["An activity", "1", "GLO", "kg", "production", "", ""],
        ["Another activity", "9.0", "here", "", "technosphere", "foo * bar + 4", "0"],
        [],
        ["Activity", "Another activity"],
        ["code", "B"],
        ["location", "here"],
        ["this", "that"],
        ["Exchanges"],
        ["name", "amount", "location", "type"],
        ["Another activity", "10", "here", "production"],
        [],
    ]
    given = CSVExtractor.extract(
        write_lci_csv("example", sections=["activities", "exchanges"])
    )[1]
    assert given == expected

    expected = [
        ["Project parameters"],
        ["name", "amount", "formula"],
        ["foo", "1.0", "green / 7"],
        ["green", "7.0", ""],
        [],
    ]
    given = CSVExtractor.extract(
        write_lci_csv(
            "example",
            sections=["project parameters", "activity parameters", "exchanges"],
        )
    )[1]
    assert given == expected


def test_excel_export_all(setup):
    write_lci_csv("example")


def test_excel_export_no_ap(setup):
    ParameterizedExchange.delete().execute()
    ActivityParameter.delete().execute()
    assert not ParameterizedExchange.select().count()
    assert not ActivityParameter.select().count()
    write_lci_csv("example")


def test_excel_export_no_dp(setup):
    ParameterizedExchange.delete().execute()
    ActivityParameter.delete().execute()
    DatabaseParameter.delete().execute()
    assert not DatabaseParameter.select().count()
    write_lci_csv("example")


def test_excel_export_no_params(setup):
    ParameterizedExchange.delete().execute()
    ActivityParameter.delete().execute()
    DatabaseParameter.delete().execute()
    ProjectParameter.delete().execute()
    assert not ProjectParameter.select().count()
    write_lci_csv("example")
