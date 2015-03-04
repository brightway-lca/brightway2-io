from ...errors import StrategyError
from ...strategies import link_iterable_by_fields
import unittest


class LinkIterableTestCase(unittest.TestCase):
    def test_raise_error_missing_fields(self):
        with self.assertRaises(StrategyError):
            link_iterable_by_fields(None, [{}])

    def test_raise_error_nonunique(self):
        data = [
            {'name': 'foo'},
            {'name': 'foo'}
        ]
        with self.assertRaises(StrategyError):
            link_iterable_by_fields(None, data)
