# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data.backends.iotable import IOTableBackend
from bw2data import Database
from ..extractors import ExiobaseDataExtractor
from ..strategies import (
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
        print("Creating database {}".format(self.db_name))
        start = time()
        self.outputs = ExiobaseDataExtractor.extract(filepath)

        print(u"Extracted {} datasets and many exchanges in {:.2f} seconds".format(
            len(self.outputs['industries']),
            time() - start
        ))

    def apply_strategies(self, biosphere=None):
        # Plus normalize units

        self.biosphere_lookup = get_biosphere_lookup_dict(
            self.outputs['substances'],
            self.outputs['extractions'],
            biosphere
        )

        print("Processing technosphere")
        self.activities = extract_exiobase_technosphere(
            self.outputs['industries'],
            self.outputs['countries'],
            self.db_name
        )

        print("Processing exchanges")
        self.exchanges = itertools.chain(
            relabel_exchanges(
                self.outputs['table'],
                self.db_name
            ),
            relabel_emissions(
                self.outputs['emissions'],
                self.db_name,
                self.biosphere_lookup
            )
        )

    def write_database(self):
        mrio = IOTableBackend(self.db_name)
        mrio.write(self.activities, self.exchanges)


def get_biosphere_lookup_dict(substances, extractions, biosphere=None):
    print("Aggregating `substances` and `extractions`")
    lookup = [{
        'name': obj[0],
        'exiobase-code': obj[1],
        'synonym': obj[2],
        'description': obj[3] or None
    } for obj in substances] + [{
        'name': obj[1],
        'synonym': obj[2]
    } for obj in extractions]

    for obj in lookup:
        obj['old-name'] = obj['name'][:]

    lookup = migrate_datasets(lookup, "exiobase-biosphere")

    biosphere_hashes = {activity_hash(obj, fields=["name", "categories"]): obj.key
                        for obj in Database(biosphere or "biosphere3")}
    return {
        obj['old-name']: biosphere_hashes[activity_hash(obj, fields=["name", "categories"])]
        for obj in lookup
    }


def extract_exiobase_technosphere(industries, countries, db_name):
    """Create activity datasets for each combination of `industries` and `countries`."""
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


def relabel_emissions(emissions_table, db_name, lookup):
    """Turn rows into a generator of (flow, process, type, amount) tuples.

    Original data format:

        (
            'AT',
            'Cultivation of wheat',
            'CO2 - combustion',
            'air',
            'kg/M.EUR',
            289687.6972210754
        )

    * `emissions_table` is the list of raw data lines.
    * `db_name` is the string name of the database, 'Exiobase 2.2' by default.
    * `lookup` is a dictionary from string flow names to biosphere keys.

    Returns:

        (
            ("biosphere3", "some-code"),  # Looks up 'CO2 - combustion' in `lookup`
            ("Exiobase 2.2", "Cultivation of wheat:AT"),
            289687.6972210754
        )

    """
    for row in emissions_table:
        yield (
            lookup[row[2]],
            (db_name, "{}:{}".format(row[1], row[0])),
            "biosphere",
            row[5]
        )


def relabel_exchanges(table, db_name):
    for row in table:
        yield (
            (db_name, "{}:{}".format(row[1], row[0])),  # Input
            (db_name, "{}:{}".format(row[3], row[2])),  # Output,
            "technosphere",
            row[5]
        )
