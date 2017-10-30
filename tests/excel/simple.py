# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io import ExcelImporter
from bw2io.importers.excel import (
    csv_restore_tuples,
    csv_restore_booleans,
    csv_numerize,
    csv_drop_unknown,
    csv_add_missing_exchanges_section,
    normalize_units,
    set_code_by_activity_hash,
    assign_only_product_as_production,
    link_technosphere_by_activity_hash,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    convert_uncertainty_types_to_integers,
)
import os

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "excel")


@bw2test
def test_parameterized_import():
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "basic_example.xlsx"))
    ei.strategies = [
        csv_restore_tuples,
        csv_restore_booleans,
        csv_numerize,
        csv_drop_unknown,
        csv_add_missing_exchanges_section,
        normalize_units,
        set_code_by_activity_hash,
        assign_only_product_as_production,
        link_technosphere_by_activity_hash,
        drop_falsey_uncertainty_fields_but_keep_zeros,
        convert_uncertainty_types_to_integers,
    ]
    ei.apply_strategies()
    ei.match_database()
    expected = []
    assert ei.data == expected
