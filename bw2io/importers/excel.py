# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..extractors import ExcelExtractor
from .csv import CSVImporter
from ..strategies import (
    add_database_name,
    assign_only_product_as_production,
    convert_uncertainty_types_to_integers,
    csv_drop_unknown,
    csv_numerize,
    csv_reformat,
    csv_restore_booleans,
    csv_restore_tuples,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    normalize_units,
    set_code_by_activity_hash,
    strip_biosphere_exc_locations,
)
from bw2data import Database, config
from time import time
import functools


class ExcelImporter(CSVImporter):
    """Generic Excel importer.

    See the `generic Excel example spreadsheet <https://bitbucket.org/cmutel/brightway2-io/raw/default/bw2io/data/examples/example.xlsx>`__.

    Excel spreadsheet should follow the following format:

    ::

        Database, <name of database>
        <database field name>, <database field value>
        <blank line>
        Activity, <name of activity>
        <database field name>, <database field value>
        Exchanges
        <field name>, <field name>, <field name>
        <value>, <value>, <value>
        <value>, <value>, <value>
        <blank line>

    Exchanges for each activity are not required.

    An activity is marked as finished with a blank line.

    In general, data is imported without modification. However, the following transformations are applied:

    * Numbers are translated from text into actual numbers.
    * Tuples, separated in the cell by the ``::`` string, are reconstructed.
    * ``True`` and ``False`` are transformed to boolean values.
    * Fields with the value ``(Unknown)`` are dropped.

    """

    format = "Excel"

    def __init__(self, filepath):
        self.strategies = [
            csv_restore_tuples,
            csv_restore_booleans,
            csv_numerize,
            csv_drop_unknown,
            normalize_units,
            normalize_biosphere_categories,
            normalize_biosphere_names,
            strip_biosphere_exc_locations,
            set_code_by_activity_hash,
            functools.partial(link_iterable_by_fields,
                other=Database(config.biosphere),
                kind='biosphere'
            ),
            assign_only_product_as_production,
            link_technosphere_by_activity_hash,
            drop_falsey_uncertainty_fields_but_keep_zeros,
            convert_uncertainty_types_to_integers,
        ]
        start = time()
        self.data = ExcelExtractor.extract(filepath)
        print("Extracted {} worksheets in {:.2f} seconds".format(
              len(self.data), time() - start))
        self.db_name, self.metadata = self.get_database(self.data)
        self.data = self.process_worksheets(self.data)

    def get_database(self, database):
        def _(x):
            if x.lower() == 'true':
                return True
            elif x.lower() == 'false':
                return False
            else:
                return x

        is_empty_line = lambda line: not line or not any(line)

        for sheet, data in database:
            if data[0][0] == 'skip':
                continue

            database_indices = [i for i, x in enumerate(data)
                                if x and x[0] == 'Database']

            if not database_indices:
                continue
            elif len(database_indices) > 1:
                raise ValueError("Multiple database metadata sections found!")
            else:
                database_index = database_indices[0]

            assert data[database_index][1], "Can't understand database name"
            name = data[database_index][1]

            metadata = {}
            for line in data[database_index:]:
                if is_empty_line(line):
                    continue
                elif line[0] == 'Activity':
                    break
                else:
                    metadata[line[0]] = _(line[1])
            return name, metadata

    def process_worksheets(self, data):
        """Take list of `(sheet names, raw data)` and process it."""
        # Skip worksheets without data
        data = [(x, y)
                     for x, y in data
                     if y[0][0].lower().strip() != 'skip']

        def cutoff(obj):
            if obj[0][0] == 'cutoff':
                try:
                    cutoff = int(obj[0][1])
                except:
                    raise ValueError("Can't understand cutoff index")
                return [x[:cutoff] for x in obj]
            else:
                return obj

        # Strip columns past cutoff
        data = [(x, cutoff(y)) for x, y in data]

        # Apply `csv_reformat` strategy to each worksheet
        data = [(x, csv_reformat(y)) for x, y in data]

        # Add worksheet and database names to each activity
        for name, obj in data:
            for ds in obj:
                ds['worksheet name'] = name
                ds['database'] = self.db_name

        return [ds for _, sheet in data for ds in sheet]
