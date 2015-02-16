from __future__ import print_function
from ..extractors import Ecospold1LCIAExtractor
from .base_lcia import LCIAImportBase
from bw2data import config
from time import time


class Ecospold1LCIAImporter(LCIAImportBase):
    format = u"Ecospold1 LCIA"

    # def go(self):
    #     self.apply_strategies()
    #     self.add_missing_cfs()
    #     self.write_methods()

    def __init__(self, filepath, biosphere=None):
        self.filepath = filepath
        self.biosphere_name = biosphere or config.biosphere
        start = time()
        self.data = Ecospold1LCIAExtractor.extract(filepath)
        print(u"Extracted {} methods in {:.2f} seconds".format(
              len(self.data), time() - start))
