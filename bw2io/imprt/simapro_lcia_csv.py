# -*- coding: utf-8 -*
from __future__ import print_function
from ..extractors import SimaProLCIACSVExtractor
from .base_lcia import LCIAImportBase
from bw2data import config
from time import time


class SimaProLCIACSVImporter(LCIAImportBase):
    format = u"SimaPro CSV LCIA"

    def __init__(self, filepath, biosphere=None, delimiter=";",
                 encoding='cp1252'):
        self.filepath = filepath
        self.biosphere_name = biosphere or config.biosphere
        start = time()
        self.data = SimaProLCIACSVExtractor.extract(filepath, delimiter, encoding)
        print(u"Extracted {} methods in {:.2f} seconds".format(
              len(self.data), time() - start))
