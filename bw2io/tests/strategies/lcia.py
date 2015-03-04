from ...strategies import add_activity_hash_code
import unittest


class LCIATestCase(unittest.TestCase):
    def test_add_activity_hash_code(self):
        data = [{
            'exchanges': [{
                'name': 'foo',
                'code': 'bar'
            }, {
                'name': 'foo',
            }]
        }]
        expected = [{
            'exchanges': [{
                'name': 'foo',
                'code': 'bar'
            }, {
                'name': 'foo',
                'code': 'acbd18db4cc2f85cedef654fccc4a4d8',
            }]
        }]
        self.assertEqual(
            expected,
            add_activity_hash_code(data)
        )
