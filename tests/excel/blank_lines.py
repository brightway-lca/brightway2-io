# -*- coding: utf-8 -*-
import os

from bw2data.parameters import *
from bw2data.tests import bw2test

from bw2io import ExcelImporter
from bw2io.importers.excel import (
    assign_only_product_as_production,
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

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")

expected = [
    {
        "categories": (
            "Chemical Manufacturing",
            "All Other Basic Organic Chemical Mnf.",
        ),
        "code": "d80f3c8f5d04d518ab02b13aca9ab7c8",
        "comment": r"100% of elementary flows and inputs from the technosphere were "
        "allocated to acetic acid, and 0% to recovered heat\n"
        "Important note: although most of the data in the US LCI database "
        "has \n"
        "undergone some sort of review, the database as a whole has not "
        "yet \n"
        "undergone a formal validation process.\n"
        "Please email comments to lci@nrel.gov.\n"
        "unspecified\n"
        "Location:  North America (US and Canada)\n"
        "Technology:  Carbonylation of methanol\n"
        "Production volume:  0",
        "database": "BW2 Excel example",
        "exchanges": [
            {
                "amount": 0.00057,
                "categories": ("air", "urban air close to ground"),
                "database": "biosphere3",
                "loc": 0.00057,
                "name": "Ammonia",
                "type": "biosphere",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 4e-05,
                "categories": ("air",),
                "database": "biosphere3",
                "loc": 4e-05,
                "name": "Methanol",
                "type": "biosphere",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 1.0,
                "categories": (
                    "Chemical Manufacturing",
                    "All Other Basic Organic Chemical Mnf.",
                ),
                "database": "BW2 Excel example",
                "loc": 1.0,
                "location": "RNA",
                "name": "Acetic acid, at plant",
                "type": "production",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 0.539,
                "categories": (
                    "Chemical Manufacturing",
                    "All Other Basic Organic Chemical Mnf.",
                ),
                "database": "BW2 Excel example",
                "loc": 0.539,
                "location": "RNA",
                "name": "Methanol, at plant",
                "type": "technosphere",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
        ],
        "filename": "/Users/cmutel/Documents/LCA Documents/US LCI "
        "database/2014/Acetic acid, at plant.xml",
        "location": "RNA",
        "name": "Acetic acid, at plant",
        "production amount": 1.0,
        "reference product": "Acetic acid, at plant",
        "type": "process",
        "unit": "kilogram",
        "worksheet name": "first process",
    },
    {
        "categories": ("Utilities", "Utilities"),
        "code": "eebaa3d205ad7646e38d346111818aad",
        "database": "BW2 Excel example",
        "exchanges": [],
        "location": "RNA",
        "name": "Electricity, at Grid, US, 2008",
        "production amount": 1.0,
        "type": "process",
        "unit": "kilowatt hour",
        "worksheet name": "other processes",
    },
    {
        "categories": (
            "Chemical Manufacturing",
            "All Other Basic Organic Chemical Mnf.",
        ),
        "code": "af33d5c5e636797a6948d5b30cf56cc0",
        "comment": "Complete inventory data and metadata are available in full in "
        "the final report and appendices, Cradle-to-Gate Life Cycle "
        "Inventory of Nine Plastic Resins and Four Polyurethane "
        "Precursors. This report has been extensively reviewed within "
        "Franklin Associates and has undergone partial critical review by "
        "ACC Plastics Division members and is available at: "
        "www.americanchemistry.com. Quantities may vary slightly between "
        "the reference to main source and and this module due to "
        "rounding.  Important note: although most of the data in the US "
        "LCI database has undergone some sort of review, the database as "
        "a whole has not yet undergone a formal validation process.  "
        "Please email comments to lci@nrel.gov.\n"
        "Includes material and energy requirements and environmental "
        "emissions for one kilogram of methanol. \n"
        "Location:  North America (US and Canada)\n"
        "Technology:  Steam reformation of light hydrocarbons followed by "
        "low pressure synthesis \n"
        "Production volume:  0\n"
        "Sampling:  Data are from primary sources.",
        "database": "BW2 Excel example",
        "exchanges": [
            {
                "amount": 5.8e-05,
                "categories": ("water", "surface water"),
                "database": "biosphere3",
                "name": "BOD5, Biological Oxygen Demand",
                "type": "biosphere",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 0.53,
                "categories": ("air",),
                "database": "biosphere3",
                "name": "Carbon dioxide, fossil",
                "type": "biosphere",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 8.8e-05,
                "categories": ("water",),
                "database": "biosphere3",
                "name": "Suspended solids, unspecified",
                "type": "biosphere",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 1.0,
                "categories": (
                    "Chemical Manufacturing",
                    "All Other Basic Organic Chemical Mnf.",
                ),
                "database": "BW2 Excel example",
                "location": "RNA",
                "name": "Methanol, at plant",
                "type": "production",
                "uncertainty type": 0,
                "unit": "kilogram",
            },
            {
                "amount": 0.00805,
                "categories": ("Utilities", "Utilities"),
                "database": "BW2 Excel example",
                "input": ("BW2 Excel example", "eebaa3d205ad7646e38d346111818aad"),
                "location": "RNA",
                "name": "Electricity, at Grid, US, 2008",
                "type": "technosphere",
                "uncertainty type": 0,
                "unit": "kilowatt hour",
            },
        ],
        "filename": "/Users/cmutel/Documents/LCA Documents/US LCI "
        "database/2014/Methanol, at plant.xml",
        "location": "RNA",
        "name": "Methanol, at plant",
        "production amount": 1.0,
        "reference product": "Methanol, at plant",
        "type": "process",
        "unit": "kilogram",
        "worksheet name": "other processes",
    },
]


@bw2test
def test_parameterized_import():
    ei = ExcelImporter(os.path.join(EXCEL_FIXTURES_DIR, "blank_lines.xlsx"))
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
    assert ei.data == expected
