from pathlib import Path

import pytest
import randonneur as rn

from bw2io.importers.base_lci import LCIImporter

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "randonneur"


def test_rd_transformation():
    imp = LCIImporter("test")
    imp.data = [{"unit": "a", "exchanges": [{"unit": "bq"}]}]
    expected = [{"unit": "a", "exchanges": [{"unit": "Becquerel"}]}]
    imp.randonneur("generic-brightway-units-normalization")
    assert imp.data == expected


def test_rn_excel_transformation():
    imp = LCIImporter("test")
    imp.data = [
        {
            "name": r"Electric arc furnace dust {Europe without Switzerland}| market for electric arc furnace dust | Cut-off, U",
            "exchanges": [
                {
                    "name": r"Electric arc furnace dust {CH}| market for electric arc furnace dust | Cut-off, U"
                }
            ],
        }
    ]
    expected = [
        {
            "reference product": "electric arc furnace dust",
            "name": "market for electric arc furnace dust",
            "location": "Europe without Switzerland",
            "exchanges": [
                {
                    "reference product": "electric arc furnace dust",
                    "name": "market for electric arc furnace dust",
                    "location": "CH",
                }
            ],
        }
    ]
    imp.randonneur(
        datapackage=rn.read_excel_template(
            FIXTURES_DIR / "randonneur-matching-template-test-fixture.xlsx"
        ),
        migrate_nodes=True,
    )
    assert imp.data == expected


def test_rn_error():
    imp = LCIImporter("test")
    imp.data = [{"unit": "a", "exchanges": [{"unit": "bq"}]}]
    with pytest.raises(ValueError):
        imp.randonneur(
            "generic-brightway-units-normalization",
            datapackage=rn.read_excel_template(
                FIXTURES_DIR / "randonneur-matching-template-test-fixture.xlsx"
            ),
        )
