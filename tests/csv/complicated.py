import os

import pytest
from bw2data.tests import bw2test

from bw2io import CSVImporter

CSV_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "csv")

@bw2test
def test_no_valid_worksheets():
    csv = CSVImporter(os.path.join(CSV_FIXTURES_DIR, "complicated.csv"))
    assert csv.db_name == 'example'
    assert csv.metadata == {'extra': 'yes please'}
    pp_expected = [
        {
            'amount': 1.0,
            'formula': 'green / 7',
            'name': 'foo'
        }, {
            'amount': 7.0,
            'name': 'green'
        },
    ]
    assert csv.project_parameters == pp_expected
    dp_expected = [
        {
            'amount': 12.0,
            'database': 'example',
            'name': 'blue'
        }, {
            'amount': 29.0,
            'database': 'example',
            'formula': '(foo + blue ** 2) / 5',
            'name': 'red'
        },
    ]
    assert csv.database_parameters == dp_expected
    data_expected = [
        {
            'code': 'A',
            'database': 'example',
            'exchanges': [{'amount': '1',
                         'location': 'GLO',
                         'name': 'An activity',
                         'type': 'production',
                         'unit': 'kg'},
                        {'amount': '9.0',
                         'formula': 'foo * bar + 4',
                         'location': 'here',
                         'name': 'Another activity',
                         'original_amount': '0',
                         'type': 'technosphere'}],
            'foo': 'bar',
            'location': 'GLO',
            'name': 'An activity',
            'parameters': {'bar': {'amount': 5.0,
                                 'code': 'A',
                                 'database': 'example',
                                 'formula': 'reference_me + 2',
                                 'group': 'my group'}},
            'unit': 'kg',
            'worksheet name': 'complicated.csv'
        }, {
            'code': 'B',
            'database': 'example',
            'exchanges': [{'amount': '10',
                         'location': 'here',
                         'name': 'Another activity',
                         'type': 'production'}],
            'location': 'here',
            'name': 'Another activity',
            'parameters': {'reference_me': {'amount': 3.0,
                                          'code': 'B',
                                          'database': 'example',
                                          'formula': 'sqrt(red - 20)',
                                          'group': 'my group'}},
            'this': 'that',
            'worksheet name': 'complicated.csv'
        },
    ]
    assert csv.data == data_expected
