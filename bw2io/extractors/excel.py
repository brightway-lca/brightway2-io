# -*- coding: utf-8 -*-
import os
import xlrd


def get_cell_value_handle_error(cell):
    if cell.ctype == 5:
        # Error type
        return None
    else:
        return cell.value


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
        return [[_(get_cell_value_handle_error(ws.cell(row, col))) for col in range(ws.ncols)] for row in range(ws.nrows)]
