from time import time

from ..extractors import SimaProLCIACSVExtractor
from ..strategies import (
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
    normalize_units,
    set_biosphere_type,
)
from .base_lcia import LCIAImporter

class SimaProLCIACSVImporter(LCIAImporter):
    """Importer for SimaPro CSV LCIA data format.

    Parameters
    ----------
    filepath : str
        Path to the SimaPro CSV LCIA file.
    biosphere : str, optional
        Name of the biosphere database to use. Default is None, which uses the current
        project's default biosphere.
    delimiter : str, optional
        Delimiter used in the CSV file. Default is ';'.
    encoding : str, optional
        Character encoding used in the CSV file. Default is 'latin-1'.
    normalize_biosphere : bool, optional
        Whether to normalize biosphere flows using the included strategies.
        Default is True.

    Notes
    -----
    This importer extracts SimaPro CSV LCIA data.

    If ``normalize_biosphere=True``, the following strategies are applied:

    * ``normalize_units``
    * ``set_biosphere_type``
    * ``normalize_simapro_biosphere_categories``
    * ``normalize_simapro_biosphere_names``

    """
    format = u"SimaPro CSV LCIA"

    def __init__(
        self,
        filepath,
        biosphere=None,
        delimiter=";",
        encoding="latin-1",
        normalize_biosphere=True,
    ):
        super(SimaProLCIACSVImporter, self).__init__(filepath, biosphere)
        if normalize_biosphere:
            self.strategies = [
                normalize_units,
                set_biosphere_type,
                normalize_simapro_biosphere_categories,
                normalize_simapro_biosphere_names,
            ] + self.strategies[2:]
        start = time()
        self.data = SimaProLCIACSVExtractor.extract(filepath, delimiter, encoding)
        print(
            u"Extracted {} methods in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )
