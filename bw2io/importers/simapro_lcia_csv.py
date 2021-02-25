from ..extractors import SimaProLCIACSVExtractor
from ..strategies import (
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
    normalize_units,
    set_biosphere_type,
)
from .base_lcia import LCIAImporter
from time import time


class SimaProLCIACSVImporter(LCIAImporter):
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
            ] + self.strategies[1:]
        start = time()
        self.data = SimaProLCIACSVExtractor.extract(filepath, delimiter, encoding)
        print(
            u"Extracted {} methods in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )
