# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..compatibility import (
    SIMAPRO_BIOSPHERE,
    ECOSPOLD_2_3_BIOSPHERE,
)
from ..units import normalize_units
from ..utils import UnicodeCSVReader, default_delimiter
from bw2data import config, Database
from functools import partial
from numbers import Number
import codecs
import copy
import gzip
import json
import os
import xlrd

dirpath = os.path.dirname(__file__)


def write_json_file(data, name):
    with codecs.open(os.path.join(dirpath, name + ".json"), "w",
                     encoding='utf-8') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def get_csv_example_filepath():
    return os.path.join(dirpath, "examples", "example.csv")


def get_xlsx_example_filepath():
    return os.path.join(dirpath, "examples", "example.xlsx")


def get_sheet(path, name):
    wb = xlrd.open_workbook(path)
    return wb.sheet_by_name(name)


def get_ecoinvent_301_31_migration_data():
    ws = get_sheet(
        os.path.join(dirpath, u"lci", u"ecoinvent 3.01-3.1.xlsx"),
        "comparison list"
    )
    deleted_activities = [
        (ws.cell(row, 0).value, ws.cell(row, 1).value)
        for row in range(1, ws.nrows)
        if ws.cell(row, 3).value == "deleted dataset"
    ]
    new_activities = [
        (ws.cell(row, 0).value, ws.cell(row, 1).value)
        for row in range(1, ws.nrows)
        if ws.cell(row, 3).value == "new dataset"
    ]
    actually_deleted = [x for x in deleted_activities if x not in new_activities]


def get_ecoinvent_2_301_migration_data():
    ws = get_sheet(
        os.path.join(dirpath, u"lci", u"ecoinvent 2-3.01.xlsx"),
        "correspondance sheet_corrected"
    )
    migration_data = [{
        '2.2 name': ws.cell(row_index, 2).value,
        'activity': ws.cell(row_index, 5).value,
        'product': ws.cell(row_index, 7).value,
        '2.2 unit': ws.cell(row_index, 10).value,
        'unit': ws.cell(row_index, 17).value,
        '2.2 location': ws.cell(row_index, 11).value,
        'location': ws.cell(row_index, 14).value,
        'conversion': ws.cell(row_index, 18).value,
    } for row_index in range(1, ws.nrows)]

    deleted_activities = [
        (ws.cell(row, 0).value, ws.cell(row, 1).value)
        for row in range(1, ws.nrows)
        if ws.cell(row, 3).value == "deleted dataset"
    ]
    new_activities = [
        (ws.cell(row, 0).value, ws.cell(row, 1).value)
        for row in range(1, ws.nrows)
        if ws.cell(row, 3).value == "new dataset"
    ]
    actually_deleted = [x for x in deleted_activities if x not in new_activities]


def get_biosphere_2_3_category_migration_data():
    """Get data for 2 -> 3 migration for biosphere flow categories"""
    return {
        'fields': ['categories', 'type'],
        'data': [
            (
                (k, 'biosphere'),  # Exchanges
                {'categories': v}
            ) for k, v in ECOSPOLD_2_3_BIOSPHERE.items()
        ] + [
            (
                (k, 'emission'),   # Datasets
                {'categories': v}
            ) for k, v in ECOSPOLD_2_3_BIOSPHERE.items()
        ]
    }


def get_biosphere_2_3_name_migration_data():
    """Get migration data for 2 -> 3 biosphere flow names.

    This migration **must** be applied only after categories have been updated.

    Note that the input data excel sheet is **modified** from the raw data provided by ecoinvent - some biosphere flows which had no equivalent in ecospold2 were mapped using my best judgment. Name changes from 3.1 were also included. Modified cells are marked in **dark orange**.

    Note that not all rows have names in ecoinvent 3. There are a few energy resources that we don't update. For water flows, the categories are updated by a different strategy, and the names don't change, so we just ignore them for now."""

    ws = get_sheet(os.path.join(dirpath, "lci", "ecoinvent elementary flows 2-3.xlsx"), "ElementaryExchanges")

    def to_exchange(obj):
        obj[0][3] = u'biosphere'
        return obj

    def strip_unspecified(one, two):
        if two == 'unspecified':
            return (one,)
        else:
            return (one, two)

    data = [
        (
            [
                ws.cell(row, 1).value,   # Old name
                # Categories
                strip_unspecified(ws.cell(row, 9).value, ws.cell(row, 10).value),
                normalize_units(ws.cell(row, 6).value),
                u'emission'  # Unit
            ], {'name': ws.cell(row, 8).value}
        )
        for row in range(1, ws.nrows)
        if ws.cell(row, 1).value
        and ws.cell(row, 8).value
        and ws.cell(row, 1).value != ws.cell(row, 8).value
    ]
    data = copy.deepcopy(data) + [to_exchange(obj) for obj in data]

    # Water unit changes
    data.extend([
        (
            ('Water', ('air',), 'kilogram', 'biosphere'),
            {'unit': 'cubic meter', 'multiplier': 0.001}
        ),
        (
            ('Water', ('air', 'non-urban air or from high stacks'), 'kilogram', 'biosphere'),
            {'unit': 'cubic meter', 'multiplier': 0.001}
        ),
        (
            ('Water', ('air', 'lower stratosphere + upper troposphere'), 'kilogram', 'biosphere'),
            {'unit': 'cubic meter', 'multiplier': 0.001}
        ),
        (
            ('Water', ('air', 'urban air close to ground'), 'kilogram', 'biosphere'),
            {'unit': 'cubic meter', 'multiplier': 0.001}
        ),
    ])

    return {
        'fields': ['name', 'categories', 'unit', 'type'],
        'data': data
    }


def get_simapro_water_migration_data():
    return json.load(open(os.path.join(dirpath, "simapro-water.json")))


def get_us_lci_migration_data():
    """Fix US LCI database name inconsistencies"""
    return {
        'fields': ['name'],
        'data': [
            (
                (k, ), {'name': v}
            ) for k, v in json.load(open(
                                         os.path.join(dirpath, "us-lci.json"),
                                         encoding='utf-8'
                                         )).items()
        ]
    }


def get_exiobase_biosphere_migration_data():
    """Migrate to ecoinvent3 flow names"""
    return json.load(open(os.path.join(dirpath, "exiomigration.json"),encoding='utf-8'))


def convert_simapro_ecoinvent_elementary_flows():
    """Write a correspondence list from SimaPro elementary flow names to ecoinvent 3 flow names to a JSON file.

    Uses custom SimaPro specific data. Ecoinvent 2 -> 3 conversion is in a separate JSON file."""
    ws = get_sheet(os.path.join(dirpath, "lci", "SimaPro - ecoinvent - biosphere.xlsx"), "ee")
    data = [[ws.cell(row, col).value for col in range(3)]
            for row in range(1, ws.nrows)]
    data = {(SIMAPRO_BIOSPHERE[obj[0]], obj[1], obj[2]) for obj in data}
    write_json_file(sorted(data), 'simapro-biosphere')


def convert_simapro_ecoinvent_3_migration_data():
    VERSIONS = (
        ("Mapping", "3.1"),
        ("Mapping 3.2", "3.2"),
        ("Mapping 3.3", "3.3"),
    )

    for ws_name, version in VERSIONS:
        ws = get_sheet(
            os.path.join(
                dirpath,
                "lci",
                "SimaPro - ecoinvent - technosphere.xlsx"
            ),
            ws_name
        )
        data = [[ws.cell(row, col).value for col in range(1, 6)]
                 for row in range(3, ws.nrows)]
        fp = os.path.join(
            dirpath,
            'lci',
            'Simapro - ecoinvent {} mapping.gzip'.format(version)
        )
        with gzip.GzipFile(fp, 'w') as fout:
            fout.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))


def get_simapro_ecoinvent_3_migration_data(version):
    """Write a migrations data file from SimaPro activity names to ecoinvent 3 processes.

    Correspondence file is processed from Pré, and has the following fields:

        #. SimaPro name
        #. Ecoinvent flow name
        #. Location
        #. Ecoinvent activity name
        #. System model
        #. SimaPro type

    Note that even the official matching data from Pré is incorrect, but works if we cast all strings to lower case.

    SimaPro type is either ``System terminated`` or ``Unit process``. We always match to unit processes regardless of SimaPro type."""
    fp = os.path.join(
        dirpath,
        'lci',
        'Simapro - ecoinvent {} mapping.gzip'.format(version)
    )
    with gzip.GzipFile(fp, 'r') as fout:
        data = json.loads(fout.read().decode("utf-8"))
    return {
        'fields': ['name'],
        'data': [(
            (line[0], ),
            {
                'location': line[2],
                'name': line[3],
                'reference product': line[1],
                'system model': line[4],
                'simapro name': line[0],
            }
        ) for line in data]
    }


def convert_ecoinvent_2_301():
    """Write a migrations data file from ecoinvent 2 to 3.1.

    This is not simple, unfortunately. We have to deal with at least the following:
        * Unit changes (e.g. cubic meters to MJ)
        * Some datasets are deleted, and replaced by others

    """
    ws = get_sheet(os.path.join(dirpath, "lci", "ecoinvent 2-3.01.xlsx"), "correspondence sheet_corrected")
    data = [[ws.cell(row, col).value for col in range(17)]
            for row in range(1, ws.nrows)]
    data = {
        'fields': ['name', 'location'],
        'data': [(
            {'name': line[0]},
            {
                'location': line[2],
                'name': line[3],
                'reference product': line[1],
                'system model': line[4]
            }
        ) for line in data]
    }
    write_json_file(data, 'simapro-ecoinvent31')


def _add_new_ecoinvent_biosphere_flows(version):
    assert version in {"33", "34", "35"}
    flows = json.load(open(os.path.join(
        os.path.dirname(__file__), "lci", "ecoinvent {} new biosphere.json".format(version)
    )))

    db = Database(config.biosphere)
    count = 0

    for flow in flows:
        flow['categories'] = tuple(flow['categories'])
        if (config.biosphere, flow['code']) not in db:
            count += 1
            db.new_activity(**flow).save()

    print("Added {} new biosphere flows".format(count))
    return db

add_ecoinvent_33_biosphere_flows = partial(_add_new_ecoinvent_biosphere_flows, version="33")
add_ecoinvent_34_biosphere_flows = partial(_add_new_ecoinvent_biosphere_flows, version="34")
add_ecoinvent_35_biosphere_flows = partial(_add_new_ecoinvent_biosphere_flows, version="35")


def convert_lcia_methods_data():
    with UnicodeCSVReader(
            os.path.join(os.path.dirname(__file__), "lcia", "categoryUUIDs.csv"),
            encoding='latin-1',
            delimiter=default_delimiter()
            ) as csv_file:
        next(csv_file)  # Skip header row
        csv_data = [{
            'name': (line[0], line[2], line[4]),
            # 'unit': line[6],
            'description': line[7]
        } for line in csv_file]

    filename = "LCIA_implementation_3.5.xlsx"
    sheet = get_sheet(
        os.path.join(dirpath, "lcia", filename),
        "CFs"
    )

    EXCLUDED = {
        'selected LCI results, additional',
        'selected LCI results',
    }

    cf_data = [{
        'method': (sheet.cell(row, 0).value,
                   sheet.cell(row, 1).value,
                   sheet.cell(row, 2).value),
        'name': sheet.cell(row, 3).value,
        'categories': (sheet.cell(row, 4).value, sheet.cell(row, 5).value),
        'amount': sheet.cell(row, 7).value
        }
        for row in range(1, sheet.nrows)
        if sheet.cell(row, 0).value not in EXCLUDED
        and isinstance(sheet.cell(row, 7).value, Number)
    ]

    sheet = get_sheet(
        os.path.join(dirpath, "lcia", filename),
        "units"
    )

    units = {
        (sheet.cell(row, 0).value,
         sheet.cell(row, 1).value,
         sheet.cell(row, 2).value): sheet.cell(row, 4).value
        for row in range(1, sheet.nrows)
    }

    return csv_data, cf_data, units, filename


def get_valid_geonames():
    """Get list of short location names used in ecoinvent 3"""
    fp = os.path.join(dirpath, "lci", "geodata.json")
    return json.load(open(fp, encoding='utf-8'))['names']


def get_ecoinvent_pre35_migration_data():
    return json.load(open(os.path.join(
        dirpath, "lci", "ecoinvent_pre35_migration.json"
    )))


def update_db_ecoinvent_locations(database_name):
    """Update ecoinvent location names for an existing database.

    Returns number of modified datasets."""
    from ..strategies.locations import GEO_UPDATE

    db = Database(database_name)
    if not len(db):
        return 0

    count = 0
    for ds in db:
        if ds['location'] in GEO_UPDATE:
            count += 1
            ds['location'] = GEO_UPDATE[ds['location']]
            ds.save()

    return count
