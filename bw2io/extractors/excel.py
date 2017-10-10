# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

import os
import xlrd


class ExcelExtractor(object):
    @classmethod
    def extract(cls, filepath):
        assert os.path.exists(filepath), "Can't file file at path {}".format(filepath)
        wb = xlrd.open_workbook(filepath)
        return [(name, cls.extract_sheet(wb, name)) for name in wb.sheet_names()]

    @classmethod
    def extract_sheet(cls, wb, name, strip=True):
        ws = wb.sheet_by_name(name)
        _ = lambda x: x.strip() if (strip and hasattr(x, "strip")) else x
        return [[_(ws.cell(row, col).value) for col in range(ws.ncols)] for row in range(ws.nrows)]

