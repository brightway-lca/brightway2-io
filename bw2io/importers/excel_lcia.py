import functools
import os

from bw2data import Database, config

from ..extractors import CSVExtractor, ExcelExtractor
from ..strategies import (
    convert_uncertainty_types_to_integers,
    csv_drop_unknown,
    csv_numerize,
    csv_restore_tuples,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    set_biosphere_type,
)
from .base_lcia import LCIAImporter


def as_dicts(obj):
    """
    Converts a 2D list to a list of dictionaries.

    Args:
        obj (list): The 2D list to be converted.

    Returns:
        list: The list of dictionaries.
    """
    if len(obj) == 1:
        obj = obj[0]
    assert isinstance(obj[0], str)
    obj = obj[1]
    return [dict(zip(obj[0], row)) for row in obj[1:]]


class ExcelLCIAImporter(LCIAImporter):
    """
    Generic Excel LCIA importer.

    Attributes:
        format (str): The file format. The default format is CSV.
        extractor (class): The file extractor class.
    
    """

    format = "Excel"
    extractor = ExcelExtractor

    def __init__(self, filepath, name, description, unit, **metadata):
        """Initializes the ExcelLCIAImporter object.

        Args:
            filepath (str): The path to the Excel file.
            name (tuple): The name of the LCIA method.
            description (str): The description of the LCIA method.
            unit (str): The unit of the LCIA method.
            **metadata: The metadata associated with the LCIA method.
        """
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
    """
    Generic CSV LCIA importer.

    Attributes:
        format (str): The file format.
        extractor (class): The file extractor class.
    """

    format = "CSV"
    extractor = CSVExtractor
