# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..extractors import ExcelExtractor, CSVExtractor
from ..strategies import (
    add_database_name,
    assign_only_product_as_production,
    convert_uncertainty_types_to_integers,
    csv_drop_unknown,
    csv_numerize,
    csv_restore_booleans,
    csv_restore_tuples,
    csv_add_missing_exchanges_section,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    normalize_units,
    set_code_by_activity_hash,
    strip_biosphere_exc_locations,
)
from .base_lci import LCIImporter
from bw2data import Database, config
from time import time
import functools


is_empty_line = lambda line: not line or not any(line)
remove_empty = lambda dct: {k: v for k, v in dct.items() if v}


class ExcelImporter(LCIImporter):
    """Generic Excel importer.

    See the `generic Excel example spreadsheet <https://bitbucket.org/cmutel/brightway2-io/raw/default/bw2io/data/examples/example.xlsx>`__.

    Excel spreadsheet should follow the following format:

    ::
        Project parameters
        <variable>, <formula>, <amount>, metadata

        Database, <name of database>
        <database field name>, <database field value>

        Parameters
        <variable>, <formula>, <amount>, metadata

        Activity, <name of activity>
        <database field name>, <database field value>
        Exchanges
        <field name>, <field name>, <field name>
        <value>, <value>, <value>
        <value>, <value>, <value>

    Neither project parameters, parameters, nor exchanges for each activity are required.

    An activity is marked as finished with a blank line.

    In general, data is imported without modification. However, the following transformations are applied:

    * Numbers are translated from text into actual numbers.
    * Tuples, separated in the cell by the ``::`` string, are reconstructed.
    * ``True`` and ``False`` are transformed to boolean values.
    * Fields with the value ``(Unknown)`` are dropped.

    """
    format = "Excel"
    extractor = ExcelExtractor

    def __init__(self, filepath):
        self.strategies = [
            csv_restore_tuples,
            csv_restore_booleans,
            csv_numerize,
            csv_drop_unknown,
            csv_add_missing_exchanges_section,
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
        data = self.extractor.extract(filepath)
        data = [(x, y) for x, y in data if hasattr(y[0][0], "lower") and y[0][0].lower() != 'skip']
        print("Extracted {} worksheets in {:.2f} seconds".format(
              len(data), time() - start))
        self.db_name, self.metadata = self.get_database(data)
        db_params = self.get_database_parameters(data)
        if db_params:
            self.metadata['parameters'] = db_params
        self.project_parameters = self.extract_project_parameters(data)
        self.data = self.process_activities(data)

    def get_database(self, data):
        results = []
        found = False
        for sn, ws in data:
            for index, line in enumerate(ws):
                if (line and hasattr(line[0], "lower") and line[0].lower() == 'database'
                    and (len(line) == 2 or not any(line[2:]))):
                    if found:
                        raise ValueError("Multiple `database` sections found")
                    results.append(self.get_metadata_section(sn, ws, index))
                    found = True

        return results[0]

    def get_database_parameters(self, data):
        results = []
        for sn, ws in data:
            for index, line in enumerate(ws):
                if (line and hasattr(line[0], "lower") and line[0].lower() == 'database parameters'
                    and (len(line) == 1 or not any(line[1:]))):
                    results.extend(self.get_labelled_section(sn, ws, index + 1))

        return results

    def extract_project_parameters(self, data):
        """Extract project parameters (variables and formulas).

        Project parameters are a section that starts with a line with the string "project parameters" (case-insensitive) in the first cell, and ends with a blank line. There can be multiple project parameter sections."""
        parameters = []
        for sn, ws in data:
            indices = []
            for index, line in enumerate(ws):
                if (line and hasattr(line[0], "lower") and line[0].lower() == 'project parameters'
                    and (len(line) == 1 or not any(line[1:]))):
                    indices.append(index)

            for index in indices:
                parameters.extend(self.get_labelled_section(sn, ws, index + 1))

        return parameters

    def get_labelled_section(self, sn, ws, index, transform=True):
        data = []

        columns = ws[index]

        for index, elem in enumerate(columns[-1:0:-1]):
            if elem:
                break
        if index:
            columns = columns[:-index]
        assert all(columns), "Missing column labels: {}:{}\n{}".format(sn, index, columns)

        index += 1
        while not (index >= len(ws) or is_empty_line(ws[index])):
            data.append({x: y for x, y in zip(columns, ws[index])})
            index += 1

        if transform:
            data = csv_restore_tuples(csv_restore_booleans(csv_numerize(csv_drop_unknown(data))))

        return [remove_empty(o) for o in data]

    def get_metadata_section(self, sn, ws, index, transform=True):
        data = {}

        name = ws[index][1]
        assert name, "Must provide valid name for metadata section (got '{}')".format(name)

        index += 1
        while not (index >= len(ws) or is_empty_line(ws[index])):
            data[ws[index][0]] = ws[index][1]
            index += 1

        if transform:
            data = csv_restore_tuples(csv_restore_booleans(csv_numerize(csv_drop_unknown([data]))))[0]

        return name, data

    def process_activities(self, data):
        """Take list of `(sheet names, raw data)` and process it."""
        new_activity = lambda x: isinstance(x[0], str) and isinstance(x[1], str) and x[0].lower() == "activity"

        def cut_worksheet(obj):
            if isinstance(obj[0][0], str) and obj[0][0].lower() == 'cutoff':
                try:
                    cutoff = int(obj[0][1])
                except:
                    raise ValueError("Can't understand cutoff index")
                return [x[:cutoff] for x in obj]
            else:
                return obj

        results = []

        for sn, ws in data:
            ws = cut_worksheet(ws)
            for index, line in enumerate(ws):
                if new_activity(line):
                    results.append(self.get_activity(sn, ws, index))

        return results

    def get_activity(self, sn, ws, start):
        exc_section = lambda x: (isinstance(x[0], str) and x[0].lower() == "exchanges"
                                 and (len(x) == 1 or not any(x[1:])))
        param_section = lambda x: (isinstance(x[0], str) and x[0].lower() == "parameters"
                                   and (len(x) == 1 or not any(x[1:])))

        ws = [row for row in ws[start:] if not is_empty_line(row)]

        for index, line in enumerate(ws):
            if param_section(line) or exc_section(line):
                end_activity_metadata = index

        name, data = self.get_metadata_section(sn, ws[:end_activity_metadata], 0, False)
        data['name'] = name
        ws = ws[end_activity_metadata:]

        if param_section(ws[0]):
            for index, line in enumerate(ws):
                if exc_section(line):
                    param_end = index

            data['parameters'] = self.get_labelled_section(sn, ws[1:param_end], 0)
            ws = ws[param_end:]

        data['exchanges'] = self.get_labelled_section(sn, ws[1:], 0, False)
        data['worksheet name'] = sn
        data['database'] = self.db_name

        return data


class CSVImporter(ExcelImporter):
    """Generic CSV importer"""
    format = "CSV"
    extractor = CSVExtractor
