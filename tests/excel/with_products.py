# -*- coding: utf-8 -*-
from bw2calc import LCA
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
import numpy as np

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


@bw2test
def test_excel_products_import():
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "with_products.xlsx"))
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
    expected = [
        {
            "code": "A",
            "database": "Product example",
            "exchanges": [
                {
                    "amount": 1.0,
                    "database": "Product example",
                    "input": ("Product example", "B"),
                    "loc": 1.0,
                    "location": "RNA",
                    "name": "Acetic acid",
                    "type": "production",
                    "uncertainty type": 0,
                    "unit": "kilogram",
                },
                {
                    "amount": 0.539,
                    "database": "Product example",
                    "input": ("Product example", "E"),
                    "loc": 0.539,
                    "location": "RNA",
                    "name": "Methanol",
                    "type": "technosphere",
                    "uncertainty type": 0,
                    "unit": "kilogram",
                },
            ],
            "location": "RNA",
            "name": "Acetic acid, at plant",
            "production amount": 1.0,
            "reference product": "Acetic acid",
            "type": "process",
            "unit": "kilogram",
            "worksheet name": "first process",
        },
        {
            "code": "B",
            "database": "Product example",
            "exchanges": [],
            "location": "RNA",
            "name": "Acetic acid",
            "type": "product",
            "unit": "kilogram",
            "worksheet name": "first process",
        },
        {
            "categories": ("Utilities", "Utilities"),
            "code": "C",
            "database": "Product example",
            "exchanges": [],
            "location": "RNA",
            "name": "Electricity, at Grid, US, 2008",
            "unit": "kilowatt hour",
            "worksheet name": "other processes",
        },
        {
            "code": "D",
            "database": "Product example",
            "exchanges": [
                {
                    "amount": 1.0,
                    "database": "Product example",
                    "input": ("Product example", "E"),
                    "location": "RNA",
                    "name": "Methanol",
                    "type": "production",
                    "uncertainty type": 0,
                    "unit": "kilogram",
                },
                {
                    "amount": 0.00805,
                    "categories": ("Utilities", "Utilities"),
                    "database": "Product example",
                    "input": ("Product example", "C"),
                    "location": "RNA",
                    "name": "Electricity, at Grid, US, 2008",
                    "type": "technosphere",
                    "uncertainty type": 0,
                    "unit": "kilowatt hour",
                },
            ],
            "location": "RNA",
            "name": "Methanol, at plant",
            "production amount": 1.0,
            "reference product": "Methanol",
            "unit": "kilogram",
            "worksheet name": "other processes",
        },
        {
            "code": "E",
            "database": "Product example",
            "exchanges": [],
            "location": "RNA",
            "name": "Methanol",
            "type": "product",
            "unit": "kilogram",
            "worksheet name": "other processes",
        },
    ]
    assert ei.data == expected


@bw2test
def test_excel_products_lca():
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "with_products.xlsx"))
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
    ei.write_database()
    lca = LCA({("Product example", "B"): 1})
    lca.lci()
    keys = {
        ("Product example", "B"),
        ("Product example", "C"),
        ("Product example", "E"),
    }
    for key in lca.product_dict:
        assert key in keys
    keys = {
        ("Product example", "A"),
        ("Product example", "C"),
        ("Product example", "D"),
    }
    for key in lca.activity_dict:
        assert key in keys
    for value in lca.supply_array:
        assert (
            np.allclose(value, 1)
            or np.allclose(value, 0.539)
            or np.allclose(value, 0.539 * 0.00805)
        )
