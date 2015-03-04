from __future__ import print_function
from ..extractors import Ecospold1LCIAExtractor
from .base_lcia import LCIAImporter
from bw2data import config
from time import time


class Ecospold1LCIAImporter(LCIAImporter):
    format = u"Ecospold1 LCIA"

    def __init__(self, filepath, biosphere=None):
        super(Ecospold1LCIAImporter, self).__init__(filepath, biosphere)
        start = time()
        self.data = Ecospold1LCIAExtractor.extract(filepath)
        print(u"Extracted {} methods in {:.2f} seconds".format(
              len(self.data), time() - start))
