# -*- coding: utf-8 -*-
from bw2data.parameters import *
from bw2data.tests import bw2test
from bw2io import ExcelImporter
from bw2io.importers.excel import (
    assign_only_product_as_production,
    convert_activity_parameters_to_list,
    convert_uncertainty_types_to_integers,
    csv_add_missing_exchanges_section,
    csv_drop_unknown,
    csv_numerize,
    csv_restore_booleans,
    csv_restore_tuples,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    link_technosphere_by_activity_hash,
    normalize_units,
    set_code_by_activity_hash,
)
import os

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


@bw2test
def test_parameterized_import():
    ei = ExcelImporter(
        os.path.join(EXCEL_FIXTURES_DIR, "sample_activities_with_variables.xlsx")
    )
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
    assert ei.project_parameters == [{"amount": 0.25, "name": "PCB_area"}]
    expected = [
        {
            "amount": 0.2,
            "maximum": 1.0,
            "minimum": 0.0,
            "name": "PCB_cap_mass_film",
            "uncertainty type": 4.0,
            "unit": "kilogram",
        },
        {
            "amount": 0.2,
            "maximum": 1.0,
            "minimum": 0.0,
            "name": "PCB_cap_mass_SMD",
            "uncertainty type": 4.0,
            "unit": "kilogram",
        },
        {
            "amount": 0.2,
            "maximum": 1.0,
            "minimum": 0.0,
            "name": "PCB_cap_mass_Tantalum",
            "uncertainty type": 4.0,
            "unit": "kilogram",
        },
    ]
    assert ei.database_parameters == expected
    expected = [
        {
            "arbitrary": "metadata",
            "code": "mpcb",
            "comment": "something important here maybe?",
            "database": "PCB",
            "exchanges": [
                {
                    "amount": 0.0,
                    "database": "PCB",
                    "formula": "PCB_area * 2",
                    "location": "GLO",
                    "name": "unmounted printed circuit board",
                    "type": "technosphere",
                    "unit": "square meter",
                },
                {
                    "amount": 0.0,
                    "database": "PCB",
                    "formula": "PCB_mass_total",
                    "location": "GLO",
                    "name": "mounted printed circuit board",
                    "type": "production",
                    "unit": "kilogram",
                },
            ],
            "location": "GLO",
            "name": "mounted printed circuit board",
            "parameters": {
                "PCB_mass_total": {
                    "amount": 0.6,
                    "group": "alpha group!",
                    "formula": "PCB_cap_mass_film + "
                    "PCB_cap_mass_SMD + "
                    "PCB_cap_mass_Tantalum",
                }
            },
            "production amount": 0.0,
            "reference product": "mounted printed circuit board",
            "type": "process",
            "unit": "kilogram",
            "worksheet name": "PCB inventory",
        },
        {
            "categories": ("electronics", "board"),
            "code": "45cb34db4147e510a2561cceec541f6b",
            "comment": "one input",
            "database": "PCB",
            "exchanges": [
                {
                    "amount": 1.0,
                    "database": "PCB",
                    "location": "GLO",
                    "name": "unmounted printed circuit board",
                    "type": "production",
                    "uncertainty type": 0,
                    "unit": "square meter",
                }
            ],
            "location": "GLO",
            "name": "unmounted printed circuit board",
            "production amount": 1.0,
            "reference product": "unmounted printed circuit board",
            "type": "process",
            "unit": "square meter",
            "worksheet name": "PCB inventory",
        },
    ]
    assert ei.data == expected


@bw2test
def test_example_notebook():
    ei = ExcelImporter(
        os.path.join(EXCEL_FIXTURES_DIR, "sample_activities_with_variables.xlsx")
    )
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
        convert_activity_parameters_to_list,
    ]
    ei.apply_strategies()
    ei.match_database(fields=["name"])
    ei.write_database()


@bw2test
def test_parameterized_import_activate_later():
    ei = ExcelImporter(
        os.path.join(EXCEL_FIXTURES_DIR, "sample_activities_with_variables.xlsx")
    )
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
        convert_activity_parameters_to_list,
    ]
    ei.apply_strategies()
    ei.match_database(fields=["name"])
    ei.write_database(activate_parameters=False)
    assert not len(parameters)
    parameters.add_to_group("some_group", ("PCB", "mpcb"))
