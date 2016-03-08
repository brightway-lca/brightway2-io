# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .base_lci import LCIImporter
from ..extractors import CSVExtractor
from ..strategies import (
    csv_drop_unknown,
    csv_numerize,
    csv_reformat,
    csv_restore_booleans,
    csv_restore_tuples,
)
from time import time


class CSVImporter(LCIImporter):
    """Generic CSV importer.

    CSV should follow the following format:

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
    * Numbers are translated from text
    * Tuples, separated in the CSV by the `::` string, are reconstructed.
    * `True` and `False` are transformed to boolean values.

    """

    format = "CSV"

    def __init__(self, filepath):
        self.strategies = [
            csv_reformat,
            csv_restore_tuples,
            csv_restore_booleans,
            csv_numerize,
            csv_drop_unknown,
        ]
        start = time()
        self.data = CSVExtractor.extract(filepath)
        count = sum([1 for x in self.data if x and x[0] == 'Activity'])
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              count, time() - start))
        self.db_name= self.get_database_name()
        self.metadata = self.extract_database_metadata()

    def get_database_name(self):
        assert self.data[0][0] == 'Database', "Must start CSV with `Database`"
        assert self.data[0][1], "Can't understand database name"
        return self.data.pop(0)[1]

    def extract_database_metadata(self):
        metadata = {}

        def _(x):
            if x.lower() == 'true':
                return True
            elif x.lower() == 'false':
                return False
            else:
                return x

        found_activity = lambda line: line and line[0] == 'Activity'

        is_empty_line = lambda line: not line or not any(line)

        while not found_activity(self.data[0]):
            line = self.data.pop(0)
            if not is_empty_line(line):
                metadata[line[0]] = _(line[1])
        return metadata

    def write_database(self):
        super(CSVImporter, self).write_database(**self.metadata)
