from ..extractors import Ecospold1LCIAExtractor
from .base_lcia import LCIAImporter
from time import time


class Ecospold1LCIAImporter(LCIAImporter):
    format = "Ecospold1 LCIA"

    def __init__(self, filepath, biosphere=None):
        super(Ecospold1LCIAImporter, self).__init__(filepath, biosphere)
        start = time()
        self.data = Ecospold1LCIAExtractor.extract(filepath)
        print(
            "Extracted {} methods in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )
