# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .base_lci import LCIImporter
from ..extractors import ExcelExtractor
from time import time


class ExcelImporter(LCIImporter):
    def __init__(self, filepath, db_name):
        self.strategies = []
        self.db_name = db_name
        start = time()
        self.data = ExcelExtractor.extract(filepath)
        print(u"Extracted {} worksheets in {:.2f} seconds".format(
              len(self.data), time() - start))
