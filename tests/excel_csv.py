# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import Database
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io import ExcelImporter
from bw2io.importers.excel import (
    csv_restore_tuples,
    csv_restore_booleans,
    csv_numerize,
    csv_drop_unknown,
    csv_add_missing_exchanges_section,
    normalize_units,
    set_code_by_activity_hash,
    assign_only_product_as_production,
    link_technosphere_by_activity_hash,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    convert_uncertainty_types_to_integers,
)
from copy import deepcopy
import os
import pytest

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "excel")


@bw2test
def test_excel_import():
    exc = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "sample_activities_with_variables.xlsx"))
    exc.strategies = [
        csv_restore_tuples,
        csv_restore_booleans,
        csv_numerize,
        csv_drop_unknown,
        csv_add_missing_exchanges_section,
        normalize_units,
        set_code_by_activity_hash,
        assign_only_product_as_production,
        link_technosphere_by_activity_hash,
        drop_falsey_uncertainty_fields_but_keep_zeros,
        convert_uncertainty_types_to_integers,
    ]
    assert exc.project_parameters
    assert exc.database_parameters
    assert exc.metadata
    assert exc.data
    exc.apply_strategies()
    exc.match_database(fields=["name"])
    exc.write_project_parameters()
    exc.write_database(activate_parameters=True)

    for o in ProjectParameter.select():
        print(o.dict)
    for o in DatabaseParameter.select():
        print(o.dict)
    for o in ActivityParameter.select():
        print(o.dict)
    for ds in Database("PCB"):
        for exc in ds.exchanges():
            print(exc)

    # raise ValueError

@pytest.fixture
def no_init(monkeypatch):
    monkeypatch.setattr(
        'bw2io.importers.excel.ExcelImporter.__init__',
        lambda x: None
    )

@bw2test
def test_get_labelled_section(no_init):
    ei = ExcelImporter()
    with pytest.raises(AssertionError):
        assert ei.get_labelled_section(None, [['', '']])
    with pytest.raises(AssertionError):
        assert ei.get_labelled_section(None, [['foo', '', 'bar']])

    data = [
        ['Parameters', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['name', 'amount', 'formula', '', '', '', '', '', '', '', '', '', '', ''],
        ['something::something', 0.6, 'A + B', '', '', '', '', '', '', '', '', '', '', ''],
        ['Nope', 1.3, '', 'Will be skipped', '', '', '', '', '', '', '', '', '', ''],
        ['', '', '', '', '', '', '', '', '', '', '', '', '', ''],
    ]
    expected = [
        {'name': 'something::something', 'amount': 0.6, 'formula': 'A + B'},
        {'name': 'Nope', 'amount': 1.3}
    ]
    assert ei.get_labelled_section(None, data, 1, transform=False) == expected
    expected = [
        {'name': ("something", "something"), 'amount': 0.6, 'formula': 'A + B'},
        {'name': 'Nope', 'amount': 1.3}
    ]
    assert ei.get_labelled_section(None, data, 1) == expected
    given = [
        ['name', 'amount', 'formula', '', '', '', '', '', '', '', '', '', '', ''],
        ['PCB_mass_total', 0.6, 'PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum', '', '', '', '', '', '', '', '', '', '', '']
    ]
    expected = [{'name': 'PCB_mass_total', 'amount': 0.6, 'formula': 'PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum'}]
    assert ei.get_labelled_section(None, given) == expected

@bw2test
def test_process_activities(no_init, monkeypatch):
    monkeypatch.setattr(
        'bw2io.importers.excel.ExcelImporter.get_activity',
        lambda a, b, c: c
    )
    ei = ExcelImporter()
    with pytest.raises(ValueError):
        assert ei.process_activities([('name', [['cutoff', 'foo']])])

    given = [('n', [
        ['cutoff', '2'],
        ['', ''],
        ['activity', 'foo', 'bar'],
        ['1', '2', '3']
    ])]
    expected = [[['activity', 'foo'], ['1', '2']]]
    assert ei.process_activities(given) == expected

@bw2test
def test_get_activity(no_init):
    given = [
        ['Activity', 'mounted printed circuit board', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['comment', 'something important here maybe?', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['arbitrary', 'metadata', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['location', 'GLO', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['type', 'process', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['unit', 'kilogram', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['Parameters', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['name', 'amount', 'formula', '', '', '', '', '', '', '', '', '', '', ''],
        ['PCB_mass_total', 0.6, 'PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum', '', '', '', '', '', '', '', '', '', '', ''],
        ['', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['Exchanges', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['name', 'amount', 'unit', 'database', 'location', 'type', 'formula', '', '', '', '', '', '', ''],
        ['unmounted printed circuit board', 0.0, 'square meter', 'PCB', 'GLO', 'technosphere', 'PCB_area * 2', 'PCB_area * 2', '', '', '', '', '', ''],
        ['mounted printed circuit board', 0.0, 'kilogram', 'PCB', 'GLO', 'production', 'PCB_mass_total', 'PCB_mass_total', '', '', '', '', '', ''],
        ['', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        ['Activity', 'unmounted printed circuit board', '', '', '', '', '', '', '', '', '', '', '', ''],
    ]
    ei = ExcelImporter()
    ei.db_name = 'db'
    expected = {
        'arbitrary': 'metadata',
        'comment': 'something important here maybe?',
        'database': 'db',
        'exchanges': [{
            'database': 'PCB',
            'amount': 0.0,
            'formula': 'PCB_area * 2',
            'location': 'GLO',
            'name': 'unmounted printed circuit board',
            'type': 'technosphere',
            'unit': 'square meter'
        }, {
            'database': 'PCB',
            'amount': 0.0,
            'formula': 'PCB_mass_total',
            'location': 'GLO',
            'name': 'mounted printed circuit board',
            'type': 'production',
            'unit': 'kilogram'
        }],
        'location': 'GLO',
        'name': 'mounted printed circuit board',
        'parameters': {'PCB_mass_total': {
            'amount': 0.6,
            'formula': 'PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum'}},
        'type': 'process',
        'unit': 'kilogram',
        'worksheet name': 'a'}
    assert ei.get_activity('a', given) == expected
