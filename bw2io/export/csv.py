# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..utils import activity_hash
from bw2data import config, Database, databases, projects
from bw2data.parameters import *
from bw2data.utils import safe_filename
import collections
import os
import csv


def reformat(value):
    if isinstance(x, (list, tuple)):
        return "::".join([reformat(x) for x in value])
    else:
        return value


class CSVFormatter(object):
    def __init__(self, database_name, objs=None, parameters=True):
        assert database_name in databases, "Database {} not found".format(database_name)
        self.get_parameters = parameters
        self.db = Database(database_name)
        self.db.order_by = 'name'
        self.objs = objs

    def get_project_parameters(self):
        return [o.dict for o in ProjectParameters.select()]

    def get_database_parameters(self):
        data = [o.dict for o in DatabaseParameters.select().where(
            DatabaseParameters.database == self.db.name)]
        return data or self.db.metadata.get("parameters")

    def get_activity_parameters(self, act):
        data = [o.dict for o in ActivityParameter.select().where(
            ActivityParameter.database == act[0],
            ActivityParameter.code == act[1],
        )]
        return data or act.get("parameters")

    def get_database_metadata(self):
        excluded = {'backend', 'depends', 'modified', 'number',
                    'processed', 'searchable', 'dirty', 'parameters'}
        data = [("Database", self.db.name)] + sorted(
               [(k, reformat(v))
                for k, v in self.db.metadata.items()
                if k not in excluded
                and not isinstance(v, (dict, list))])
        parameters = self.get_database_metadata()
        if parameters and self.get_parameters:
            data.extend([[], ["Database parameters"]] + parameters)

        pp = self.get_project_parameters()
        if pp and self.get_parameters:
            data = [["Project parameters"]] + pp + [[]] + data
        return data

    def get_activity_metadata(self, act):
        excluded = {"database", "name"}
        return [("Activity", act.get("name"))] + sorted(
               [(k, reformat(v))
                for k, v in act.items()
                if k not in excluded
                and not isinstance(v, (dict, list))])

    def get_columns_for_exc(self, exc, fields):
        inp = exc.input
        inp_fields = ("name", "unit", "location", "categories")
        choose_obj = lambda x: inp if field in inp_fields else exc
        return [reformat(choose_obj.get(field)) for field in fields]

    def get_activity_exchanges(self, act):
        exchanges = list(act.exchanges())
        exchanges.sort(key=lambda x: (x.get("type"), x.input.get("name")))

        columns = [
            "name",
            "amount",
            "formula",
            "database",
            "location",
            "unit",
            "categories",
            "type",
            "uncertainty type",
            "loc",
            "scale",
            "shape",
            "minimum",
            "maximum",
        ]
        fields = {key for exc in exchanges for key in exc._data}
        columns = [x for x in columns if x in fields]
        fields = sorted(fields.difference(set(columns)))

        return [columns + fields] + sorted([
            self.get_columns_for_exc(exc, columns + fields)
            for exc in exchanges
        ])

    def get_activity(self, act, blank_line=False):
        data = self.get_activity_metadata(act)
        params = self.get_activity_parameters(act)
        if params and self.get_parameters:
            data.exten([["Parameters"]] + params)
        excs = self.get_activity_exchanges(act)
        if excs:
            data.extend([['Exchanges']] + excs)
        if blank_line:
            data.append([])
        return data

    def get_formatted_data(self):
        if self.get_parameters:
            data = self.get_project_parameters() + [[]]
        else:
            data = []
        data.extend(self.get_database_metadata())
        data.append([])
        for act in (self.objs or self.db):
            data.extend(self.get_activity(act, True))
        return data


def write_lci_csv(database_name, parameters=True):
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
    data = CSVFormatter(database_name, parameters=parameters).get_formatted_data()

    safe_name = safe_filename(database_name, False)
    filepath = os.path.join(projects.output_dir, "lci-" + safe_name + ".csv")

    with open(filepath, "w") as f:
        writer = csv.writer(f)
        for line in data:
            writer.writerow(line)

    return filepath
