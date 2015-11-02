# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data.backends.iotable import IOTableBackend
from bw2data import Database
from ..extractors import ExiobaseDataExtractor
from ..strategies import (
    # normalize_biosphere_categories,
    # normalize_biosphere_names,
    # normalize_units,
    # link_iterable_by_fields,
    migrate_datasets,
)
from ..utils import activity_hash
from bw2data import config
from time import time
import functools
import itertools
import pprint


class Exiobase22Importer(object):
    format = u"Exiobase 2.2"

    def __init__(self, filepath, db_name="EXIOBASE 2.2"):
        self.strategies = []
        self.db_name = db_name
        start = time()
        self.outputs = ExiobaseDataExtractor.extract(filepath)

        print(u"Extracted {} datasets and many exchanges in {:.2f} seconds".format(
            len(self.outputs['industries']),
            time() - start
        ))

    def process_raw_data(self, biosphere="biosphere3"):
        print("Aggregating `substances` and `extractions`")
        self.biosphere = [{
            'name': obj[0],
            'exiobase-code': obj[1],
            'synonym': obj[2],
            'description': obj[3] or None
        } for obj in self.outputs['substances']] + [{
            'name': obj[1],
            'synonym': obj[2]
        } for obj in self.outputs['extractions']]

        for obj in self.biosphere:
            obj['old-name'] = obj['name'][:]

        self.biosphere = migrate_datasets(self.biosphere, "exiobase-biosphere")

        biosphere_hashes = {activity_hash(obj, fields=["name", "categories"]): obj.key for obj in Database(biosphere)}
        self.biosphere = {
            obj['old-name']: biosphere_hashes[activity_hash(obj, fields=["name", "categories"])]
            for obj in self.biosphere
        }

    def apply_strategies(self):
        print("Processing biosphere")
        self.biosphere = extract_exiobase_biosphere(
            self.outputs['emissions'] + self.outputs['resources'],
            self.outputs['substances'],
            self.db_name
        )

        # Plus normalize units
        print("Processing technosphere")
        self.activities = extract_exiobase_technosphere(
            self.outputs['industries'],
            self.outputs['countries'],
            self.db_name
        )

        # Plus normalize units
        print("Processing exchanges")
        self.exchanges = itertools.chain(
            relabel_exchanges(
                self.outputs['table'],
                self.db_name
            ),
            relabel_emissions(
                self.outputs['emissions'],
                self.db_name
            )
        )

    def write_databases(self):
        bd = Database("{} biosphere".format(self.db_name))
        bd.write(self.biosphere)
        mrio = IOTableBackend(self.db_name)
        mrio.write(self.activities, self.exchanges)


def extract_exiobase_biosphere(emissions_table, substances_table, db_name):
    # Get list of all possible biosphere exchanges
    biosphere_set = {(line[2], line[3], line[4]) for line in emissions_table}
    substance_dict = {obj[0]: obj for obj in substances_table}
    biosphere = [{
        'name': obj[0],
        'code': obj[0],
        'exiobase_code': substance_dict[obj[0]][1],
        'database': "{} biosphere".format(db_name),
        'categories': [obj[1]],
        'synonym': substance_dict[obj[0]][2],
        'unit': obj[2].replace("/M.EUR", ""),
        'type': 'emission',
    } for obj in biosphere_set]
    return {(obj['database'], obj['code']): obj for obj in biosphere}


def extract_exiobase_technosphere(industries, countries, db_name):
    data = [{
        'name': obj[1],
        'code': obj[1],
        'key': (db_name, "{}:{}".format(obj[1], country[0])),
        'exiobase_code': obj[0],
        'database': db_name,
        'synonym': obj[2],
        'location': country[1],
        'group': obj[3],
        'group_name': obj[4],
        'unit': 'million €',
        'type': 'process',
    } for country in countries for obj in industries]
    return {obj['key']: obj for obj in data}


def relabel_emissions(emissions_table, db_name):
    data = [
        (
            ("{} biosphere".format(db_name), row[2]),
            (db_name, "{}:{}".format(row[1], row[0])),
            "biosphere",
            row[5]
        ) for row in emissions_table
    ]
    return data


def relabel_exchanges(table, db_name):
    for row in table:
        yield (
            (db_name, "{}:{}".format(row[1], row[0])),  # Input
            (db_name, "{}:{}".format(row[3], row[2])),  # Output,
            "technosphere",
            row[5]
        )
