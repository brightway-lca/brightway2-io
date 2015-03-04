# -*- coding: utf-8 -*
from __future__ import print_function
from ..extractors import SimaProLCIACSVExtractor
from ..strategies import normalize_simapro_lcia_biosphere_categories
from .base_lcia import LCIAImportBase
from bw2data import config
from time import time


class SimaProLCIACSVImporter(LCIAImportBase):
    format = u"SimaPro CSV LCIA"

    def __init__(self, filepath, biosphere=None, delimiter=";",
                 encoding='cp1252', normalize_biosphere=True):
        super(SimaProLCIACSVImporter, self).__init__(filepath, biosphere)
        if normalize_biosphere:
            self.strategies.insert(
                0,
                normalize_simapro_lcia_biosphere_categories
            )
        start = time()
        self.data = SimaProLCIACSVExtractor.extract(filepath, delimiter, encoding)
        print(u"Extracted {} methods in {:.2f} seconds".format(
              len(self.data), time() - start))
