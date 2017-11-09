# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..utils import activity_hash
from bw2data import config, Database, databases, projects
from bw2data.parameters import ActivityParameter, DatabaseParameter, ProjectParameter
from bw2data.utils import safe_filename
import collections
import os
import csv


def reformat(value):
    if isinstance(value, (list, tuple)):
        return "::".join([reformat(x) for x in value])
    else:
        return value


EXCHANGE_COLUMNS = [
    "name",
    "amount",
    "database",
    "location",
    "unit",
    "categories",
    "type",
    "formula",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
]
PARAMETER_COLUMNS = [
    "name",
    "amount",
    "formula",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
]
MAPPING = {
    'exchange': EXCHANGE_COLUMNS,
    'parameter': PARAMETER_COLUMNS,
}


class CSVFormatter(object):
    def __init__(self, database_name, objs=None):
        assert database_name in databases, "Database {} not found".format(database_name)
        self.db = Database(database_name)
        self.db.order_by = 'name'
        self.objs = objs or iter(self.db)

    def get_project_parameters(self):
        return self.order_dicts(
            [o.dict for o in ProjectParameter.select()],
            'parameter'
        )

    def get_database_parameters(self):
        data = [o.dict for o in DatabaseParameter.select().where(
            DatabaseParameter.database == self.db.name)]
        return self.order_dicts(data, 'parameter')

    def get_activity_parameters(self, act):
        data = [o.dict for o in ActivityParameter.select().where(
            ActivityParameter.database == act[0],
            ActivityParameter.code == act[1],
        )]
        if not data:
            return {}
        dct = self.order_dicts(data, 'parameter')
        dct['group'] = ActivityParameter.get(
            database=act[0], code=act[1],
        ).group
        return dct

    def get_database_metadata(self):
        excluded = {'backend', 'depends', 'modified', 'number',
                    'processed', 'searchable', 'dirty', 'parameters'}
        return {
            'name': self.db.name,
            'metadata': sorted([(k, reformat(v))
                for k, v in self.db.metadata.items()
                if k not in excluded
                and not isinstance(v, (dict, list))
            ]),
            'parameters': self.get_database_parameters(),
            'project parameters': self.get_project_parameters()
        }

    def get_activity_metadata(self, act):
        excluded = {"database", "name"}
        return {
            'name': act.get("name"),
            'metadata': sorted([(k, reformat(v))
                for k, v in act.items()
                if k not in excluded
                and not isinstance(v, (dict, list))
            ]),
            'parameters': self.get_activity_parameters(act)
        }

    def exchange_as_dict(self, exc):
        inp = exc.input
        inp_fields = ("name", "unit", "location", "categories")
        skip_fields = ("input", "output")
        data = {k: v for k, v in exc._data.items()
                if k not in skip_fields}
        data.update(**{k: inp[k] for k in inp_fields if inp.get(k)})
        return data

    def order_dicts(self, data, kind="exchange"):
        if not data:
            return {}
        found = {obj for dct in data for obj in dct}
        used_columns = [x for x in MAPPING[kind] if x in found]
        extra_fields = sorted(found.difference(set(used_columns)))
        columns = used_columns + extra_fields
        return {
            'columns': columns,
            'data': [[reformat(dct.get(c)) for c in columns]
                     for dct in data]
        }

    def get_exchanges(self, act):
        exchanges = [self.exchange_as_dict(exc) for exc in act.exchanges()]
        exchanges.sort(key=lambda x: (x.get("type"), x.get("name")))
        return self.order_dicts(exchanges)

    def get_activity(self, act):
        data = self.get_activity_metadata(act)
        data['exchanges'] = self.get_exchanges(act)
        return data

    def get_unformatted_data(self):
        """Return all database data as a nested dictionary:

        .. code-block:: python

            {
                'database': {
                    'name': name,
                    'metadata': [(key, value)],
                    'parameters': {
                        'columns': [column names],
                        'data': [[column values for each row]]
                    },
                    'project parameters': {
                        'columns': [column names],
                        'data': [[column values for each row]]
                    }
                },
                'activities': [{
                    'name': name,
                    'metadata': [(key, value)],
                    'parameters': {
                        'columns': [column names],
                        'group': 'group name',
                        'data': [[column values for each row]]
                    },
                    'exchanges': {
                        'columns': [column names],
                        'data': [[column values for each row]]
                    }
                }]
            }

        """
        return {
            'database': self.get_database_metadata(),
            'activities': [self.get_activity(obj) for obj in self.objs]
        }

    def get_formatted_data(self, sections=None):
        if sections is None:
            sections = [
                'project parameters', 'database', 'database parameters',
                'activities', 'activity parameters', 'exchanges'
            ]

        result = []
        data = self.get_unformatted_data()
        db = data['database']
        if db['project parameters'] and 'project parameters' in sections:
            result.extend([
                ['Project parameters'],
                db['project parameters']['columns']
            ])
            result.extend(db['project parameters']['data'])
            result.append([])

        if 'database' in sections:
            result.append(['Database', db['name']])
            result.extend(db['metadata'])
            result.append([])

        if db['parameters'] and 'database parameters' in sections:
            result.extend([
                ['Database parameters'],
                db['parameters']['columns']
            ])
            result.extend(db['parameters']['data'])
            result.append([])

        if 'activities' not in sections:
            return result
        for act in data['activities']:
            result.append(['Activity', act['name']])
            result.extend(act['metadata'])

            if act['parameters'] and 'activity parameters' in sections:
                result.extend([
                    ['Parameters', act['parameters']['group']],
                    act['parameters']['columns']
                ])
                result.extend(act['parameters']['data'])
                result.append([])

            if 'exchanges' in sections:
                result.append(['Exchanges'])
                if act['exchanges']:
                    result.append(act['exchanges']['columns'])
                    result.extend(act['exchanges']['data'])

            result.append([])
        return result


def write_lci_csv(database_name, sections=None):
    """Export database `database_name` to a CSV file.

    Not all data can be exported. The following constraints apply:

    * Nested data, e.g. `{'foo': {'bar': 'baz'}}` are excluded. CSV is not a great format for nested data. However, *tuples* are exported, and the characters `::` are used to join elements of the tuple.
    * The only well-supported data types are strings, numbers, and booleans.

    Returns the filepath of the exported file.

    """
    data = CSVFormatter(database_name).get_formatted_data(sections)

    safe_name = safe_filename(database_name, False)
    filepath = os.path.join(projects.output_dir, "lci-" + safe_name + ".csv")

    with open(filepath, "w") as f:
        writer = csv.writer(f)
        for line in data:
            writer.writerow(line)

    return filepath
