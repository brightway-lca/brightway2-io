from ..extractors import ExcelExtractor, CSVExtractor
from ..strategies import (
    convert_uncertainty_types_to_integers,
    csv_drop_unknown,
    csv_numerize,
    csv_restore_tuples,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    link_iterable_by_fields,
    set_biosphere_type,
    drop_unspecified_subcategories,
)
from .base_lcia import LCIAImporter
from bw2data import Database, config
import functools
import os


def as_dicts(obj):
    if len(obj) == 1:
        obj = obj[0]
    assert isinstance(obj[0], str)
    obj = obj[1]
    return [dict(zip(obj[0], row)) for row in obj[1:]]


class ExcelLCIAImporter(LCIAImporter):
    """Generic Excel LCIA importer.

    See the `generic Excel LCIA example spreadsheet <https://bitbucket.org/cmutel/brightway2-io/raw/default/bw2io/data/examples/example_lcia.xlsx>`__.

    Excel LCIA spreadsheets should have a first row of column labels, and follow the following format:

    ::

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

    def __init__(self, filepath, name, description, unit, **metadata):
        assert isinstance(name, tuple)
        self.strategies = [
            csv_restore_tuples,
            csv_numerize,
            csv_drop_unknown,
            set_biosphere_type,
            drop_unspecified_subcategories,
            functools.partial(
                link_iterable_by_fields,
                other=Database(config.biosphere),
                kind="biosphere",
                fields=("name", "categories"),
            ),
            drop_falsey_uncertainty_fields_but_keep_zeros,
            convert_uncertainty_types_to_integers,
        ]
        self.data = [
            {
                "name": name,
                "description": description,
                "filename": os.path.basename(filepath),
                "unit": unit,
                "exchanges": as_dicts(self.extractor.extract(filepath)),
            }
        ]
        for ds in self.data:
            ds.update(**metadata)


class CSVLCIAImporter(ExcelLCIAImporter):
    """Generic CSV LCIA importer"""

    format = "CSV"
    extractor = CSVExtractor
