# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import Database
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io.export.csv import CSVFormatter, write_lci_csv
from bw2io.export.excel import write_lci_excel
from bw2io.extractors.csv import CSVExtractor
from bw2io.extractors.excel import ExcelExtractor
import os
import pytest

CSV_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "csv")
EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


@pytest.fixture
@bw2test
def setup():
    db = Database("example")
    db.register(extra='yes please')

    a = db.new_activity(code="A", name="An activity", unit='kg', foo='bar')
    a.save()
    a.new_exchange(amount=1, input=a, type="production").save()

    b = db.new_activity(code="B", name="Another activity", location='here', this='that')
    b.save()
    b.new_exchange(amount=10, input=b, type="production").save()
    a.new_exchange(amount=0, input=b, type="technosphere", formula="foo * bar + 4").save()

    project_data = [{
        'name': 'foo',
        'formula': 'green / 7',
    }, {
        'name': 'green',
        'amount': 7
    }]
    parameters.new_project_parameters(project_data)

    database_data = [{
        'name': 'red',
        'formula': '(foo + blue ** 2) / 5',
    }, {
        'name': 'blue',
        'amount': 12
    }]
    parameters.new_database_parameters(database_data, "example")

    activity_data = [{
        'name': 'reference_me',
        'formula': 'sqrt(red - 20)',
        'database': 'example',
        'code': "B",
    }, {
        'name': 'bar',
        'formula': 'reference_me + 2',
        'database': 'example',
        'code': "A",
    }]
    parameters.new_activity_parameters(activity_data, "my group")

    parameters.add_exchanges_to_group("my group", a)
    ActivityParameter.recalculate_exchanges("my group")

def test_write_lci_csv_complicated(setup):
    given = CSVExtractor.extract(write_lci_csv("example"))[1]
    expected = CSVExtractor.extract(os.path.join(CSV_FIXTURES_DIR, 'complicated.csv'))[1]
    assert given == expected

def test_write_lci_excel_complicated(setup):
    fp = write_lci_excel("example")
    given = ExcelExtractor.extract(fp)[0][1]
    expected = ExcelExtractor.extract(os.path.join(EXCEL_FIXTURES_DIR, 'export-complicated.xlsx'))[0][1]
    assert given == expected
