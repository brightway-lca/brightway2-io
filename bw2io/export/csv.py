# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..utils import activity_hash
from bw2data import config, Database, databases, projects
from bw2data.utils import safe_filename
import collections
import os
import csv

_ = lambda x: "::".join(x) if isinstance(x, (list, tuple)) else x

class CSVFormatter(object):
    def __init__(self, database_name, objs=None):
        assert database_name in databases, "Database {} not found".format(database_name)
        self.db = Database(database_name)
        self.db.order_by = 'name'
        self.objs = objs

    def get_database_metadata(self):
        excluded = {'backend', 'depends', 'modified', 'number',
                    'processed', 'searchable', 'dirty'}
        return [("Database", self.db.name)] + sorted(
               [(k, _(v))
                for k, v in self.db.metadata.items()
                if k not in excluded
                and not isinstance(v, (dict, list))])

    def get_activity_metadata(self, act):
        excluded = {"database", "name"}
        return [("Activity", act.get("name"))] + sorted(
               [(k, _(v))
                for k, v in act.items()
                if k not in excluded
                and not isinstance(v, (dict, list))])

    def get_columns_for_exc(self, exc):
        inp = exc.input
        return (
            inp.get('name', '(Unknown)'),
            exc['amount'],
            inp.get('unit', '(Unknown)'),
            exc['input'][0],
            _(inp.get('categories', '(Unknown)')),
            inp.get('location', '(Unknown)'),
            exc.get('type', '(Unknown)'),
            exc.get('uncertainty type', '(Unknown)'),
            exc.get('loc', '(Unknown)'),
            exc.get('scale', '(Unknown)'),
            exc.get('shape', '(Unknown)'),
            exc.get('minimum', '(Unknown)'),
            exc.get('maximum', '(Unknown)'),
        )

    def get_activity_exchanges(self, act):
        exchanges = list(act.exchanges())
        exchanges.sort(key=lambda x: (x.get("type"), x.input.get("name")))

        columns = (
            "name",
            "amount",
            "unit",
            "database",
            "categories",
            "location",
            "type",
            "uncertainty type",
            "loc",
            "scale",
            "shape",
            "minimum",
            "maximum",
        )

        return [columns] + [self.get_columns_for_exc(exc) for exc in exchanges]

    def get_formatted_data(self):
        data = self.get_database_metadata()
        data.append(())
        for act in (self.objs or self.db):
            data.extend(self.get_activity_metadata(act))
            if len(act.exchanges()):
                data.append(("Exchanges",))
                data.extend(self.get_activity_exchanges(act))
            data.append(())

        return data


def write_lci_csv(database_name):
    """Export database `database_name` to a CSV file.

    Not all data can be exported. The following constraints apply:

    * Nested data, e.g. `{'foo': {'bar': 'baz'}}` are excluded. CSV is not a great format for nested data. However, *tuples* are exported, and the characters `::` are used to join elements of the tuple.
    * Only the following fields in exchanges are exported:
        * name
        * amount
        * unit
        * database
        * categories
        * location
        * type
        * uncertainty type
        * loc
        * scale
        * shape
        * minimum
        * maximum
    * The only well-supported data types are strings, numbers, and booleans.

    Returns the filepath of the exported file.

    """
    data = CSVFormatter(database_name).get_formatted_data()

    safe_name = safe_filename(database_name, False)
    filepath = os.path.join(projects.output_dir, "lci-" + safe_name + ".csv")

    with open(filepath, "w") as f:
        writer = csv.writer(f)
        for line in data:
            writer.writerow(line)

    return filepath
