from bw2io.extractors.excel import ExcelExtractor
import os


EXCEL_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "excel")


def test_parse_formulas():
    data = ExcelExtractor.extract(os.path.join(EXCEL_FIXTURES_DIR, "with_formulas.xlsx"))
    print(data)
    expected = [('worksheet', [[1], [2]])]
    assert data == expected
