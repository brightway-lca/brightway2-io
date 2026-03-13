import os

import pytest

from bw2io import ExcelImporter

EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


def test_excel_importer_sheet_name_filters():
    path = os.path.join(EXCEL_FIXTURES_DIR, "basic_example.xlsx")

    full = ExcelImporter(path)
    filtered = ExcelImporter(path, sheet_name="first process")

    assert filtered.data
    assert all(ds.get("worksheet name") == "first process" for ds in filtered.data)
    assert len(filtered.data) < len(full.data)


def test_excel_importer_sheet_name_unknown_raises():
    path = os.path.join(EXCEL_FIXTURES_DIR, "basic_example.xlsx")

    with pytest.raises(ValueError):
        ExcelImporter(path, sheet_name="does-not-exist")
