# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io import ExcelImporter
from bw2data.tests import bw2test
import os

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "excel")

@bw2test
def test_excel_import():
    exc = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "sample_activities_with_variables.xlsx"))
