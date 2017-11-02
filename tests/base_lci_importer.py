# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import Database
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io import ExcelImporter
from bw2io.importers.base_lci import LCIImporter
import pytest
import numpy as np

### Tests TODO
# Activity parameters without group names
# Activity parameters with group names
# Activity parameters - multiple activities with same group name
# Test delete database parameters
# Test skip delete if parameters are None
# Test update database parameters
# Delete stale activity parameters with wrong (old) group
# Test delete activity parameters (right group, delete_existing)
# Test update activity parameters
# Test error formatting for wrongdatabase
# Test error formatting for nonuniquecode
# Test database update existing data

DATA = [
         {'arbitrary': 'metadata',
          'code': '32aa5ab78beda5b8c8efbc89587de7a5',
          'comment': 'something important here maybe?',
          'database': 'PCB',
          'exchanges': [{'amount': 0.0,
                         'input': ('PCB', '45cb34db4147e510a2561cceec541f6b'),
                         'formula': 'PCB_area * 2',
                         'location': 'GLO',
                         'name': 'unmounted printed circuit board',
                         'type': 'technosphere',
                         'unit': 'square meter'},
                        {'amount': 0.0,
                         'input': ('PCB', '32aa5ab78beda5b8c8efbc89587de7a5'),
                         'formula': 'PCB_mass_total',
                         'location': 'GLO',
                         'name': 'mounted printed circuit board',
                         'type': 'production',
                         'unit': 'kilogram'}],
          'location': 'GLO',
          'name': 'mounted printed circuit board',
          'parameters': {'PCB_mass_total': {'amount': 0.6,
                                            'formula': 'PCB_cap_mass_film + '
                                                       'PCB_cap_mass_SMD + '
                                                       'PCB_cap_mass_Tantalum'}},
          'production amount': 0.0,
          'reference product': 'mounted printed circuit board',
          'type': 'process',
          'unit': 'kilogram',
          'worksheet name': 'PCB inventory'},
         {'categories': ('electronics', 'board'),
          'code': '45cb34db4147e510a2561cceec541f6b',
          'comment': 'one input',
          'database': 'PCB',
          'exchanges': [{'amount': 1.0,
                         'input': ("PCB", '45cb34db4147e510a2561cceec541f6b'),
                         'location': 'GLO',
                         'name': 'unmounted printed circuit board',
                         'type': 'production',
                         'uncertainty type': 0,
                         'unit': 'square meter'}],
          'location': 'GLO',
          'name': 'unmounted printed circuit board',
          'production amount': 1.0,
          'reference product': 'unmounted printed circuit board',
          'type': 'process',
          'unit': 'square meter',
          'worksheet name': 'PCB inventory'}
]
DB = [{
    'amount': 0.2,
    'maximum': 1.0,
    'minimum': 0.0,
    'name': 'PCB_cap_mass_film',
    'uncertainty type': 4.0,
    'unit': 'kilogram'
}, {
    'amount': 0.2,
    'maximum': 1.0,
    'minimum': 0.0,
    'name': 'PCB_cap_mass_SMD',
    'uncertainty type': 4.0,
    'unit': 'kilogram'
}, {
    'amount': 0.2,
    'maximum': 1.0,
    'minimum': 0.0,
    'name': 'PCB_cap_mass_Tantalum',
    'uncertainty type': 4.0,
    'unit': 'kilogram'
}]


@pytest.fixture
@bw2test
def lci():
    obj = LCIImporter("PCB")
    obj.project_parameters = [{'amount': 0.25, 'name': 'PCB_area'}]
    obj.data = DATA
    obj.database_parameters = DB
    return obj


def test_write_database_no_activate(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=False)
    assert [g.name for g in Group.select()] == ["project"]

def test_write_database(lci):
    lci.write_project_parameters()
    lci.write_database(activate_parameters=True)
    for g in Group.select():
        print(g.name)
    assert sorted([g.name for g in Group.select()]) == ["PCB", 'PCB:32aa5ab78beda5b8c8efbc89587de7a5', "project"]

    assert ActivityParameter.select().count() == 1
    for x in ActivityParameter.select():
        found = x.dict
    assert found['database'] == 'PCB'
    assert found['code'] == '32aa5ab78beda5b8c8efbc89587de7a5'
    assert found['formula'] == 'PCB_cap_mass_film + PCB_cap_mass_SMD + PCB_cap_mass_Tantalum'
    assert np.allclose(found['amount'], 0.6)

    given = [
        {'database': 'PCB', 'name': 'PCB_cap_mass_film', 'amount': 0.2, 'maximum': 1.0, 'minimum': 0.0, 'uncertainty type': 4.0, 'unit': 'kilogram'},
        {'database': 'PCB', 'name': 'PCB_cap_mass_SMD', 'amount': 0.2, 'maximum': 1.0, 'minimum': 0.0, 'uncertainty type': 4.0, 'unit': 'kilogram'},
        {'database': 'PCB', 'name': 'PCB_cap_mass_Tantalum', 'amount': 0.2, 'maximum': 1.0, 'minimum': 0.0, 'uncertainty type': 4.0, 'unit': 'kilogram'}
    ]

    assert DatabaseParameter.select().count() == 3
    for x in DatabaseParameter.select():
        assert x.dict in given

    assert ProjectParameter.select().count() == 1
    for x in ProjectParameter.select():
        assert x.dict == {'name': 'PCB_area', 'amount': 0.25}

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
