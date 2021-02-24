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

    See the `documentation <https://2.docs.brightway.dev/intro.html#importing-lcia-methods-from-the-standard-excel-template>`__.

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
