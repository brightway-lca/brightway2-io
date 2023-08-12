import pytest
from bw2data import Database, get_node
from bw2data.parameters import *
from bw2data.tests import bw2test

from bw2io.data import update_db_ecoinvent_locations
from bw2io.strategies import update_ecoinvent_locations


def test_locations_update():
    given = [
        {
            "location": "Foo",
            "exchanges": [
                {
                    "location": "WECC, US only",
                }
            ],
        },
        {"location": "SGCC"},
    ]
    expected = [
        {
            "location": "Foo",
            "exchanges": [
                {
                    "location": "US-WECC",
                }
            ],
        },
        {"location": "CN-SGCC"},
    ]
    assert update_ecoinvent_locations(given) == expected


@bw2test
def test_existing_db_locations_update():
    db = Database("foo")
    db.write(
        {
            ("foo", "1"): {"location": "nowhere", "name": "b"},
            ("foo", "2"): {"location": "SGCC", "name": "a"},
        }
    )
    assert get_node(code="2")["location"] == "SGCC"
    assert update_db_ecoinvent_locations("foo") == 1
    assert get_node(code="2")["location"] == "CN-SGCC"
    assert update_db_ecoinvent_locations("bar") == 0
