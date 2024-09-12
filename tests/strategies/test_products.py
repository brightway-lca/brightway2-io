from copy import deepcopy

import bw2data as bd
import pytest

from bw2io.strategies import create_products_as_new_nodes


def test_create_products_as_new_nodes_basic():
    data = [
        {
            "name": "epsilon",
            "location": "there",
        },
        {
            "name": "alpha",
            "database": "foo",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                }
            ],
        },
    ]
    original = deepcopy(data)
    result = create_products_as_new_nodes(data)
    assert len(data) == 3
    original[1]["exchanges"][0]["input"] = (result[2]["database"], result[2]["code"])
    assert result[:2] == original[:2]
    product = {
        "database": "foo",
        "code": result[2]["code"],
        "name": "beta",
        "unit": "kg",
        "location": "here",
        "exchanges": [],
        "type": bd.labels.product_node_default,
        "extra": True,
    }
    assert result[2] == product


def test_create_products_as_new_nodes_ignore_multifunctional():
    data = [
        {
            "name": "alpha",
            "database": "foo",
            "type": bd.labels.multifunctional_node_default,
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                }
            ],
        }
    ]
    create_products_as_new_nodes(data)
    assert len(data) == 1


def test_create_products_as_new_nodes_skip_nonqualifying():
    data = [
        {
            "name": "epsilon",
            "location": "there",
        },
        {
            "name": "alpha",
            "database": "foo",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                },
                {
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                },
                {
                    "name": "gamma",
                    "unit": "kg",
                    "location": "here",
                    "functional": False,
                    "type": "production",
                    "extra": True,
                },
                {
                    "name": "delta",
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "input": ("foo", "bar"),
                },
                {
                    "name": "epsilon",
                    "unit": "kg",
                    "location": "there",
                    "functional": True,
                    "type": "technosphere",
                },
            ],
        },
    ]
    original = deepcopy(data)
    result = create_products_as_new_nodes(data)
    assert len(data) == 3
    original[1]["exchanges"][0]["input"] = (result[2]["database"], result[2]["code"])
    assert result[:2] == original[:2]
    assert result[2]["name"] == "beta"


def test_create_products_as_new_nodes_duplicate_exchanges():
    data = [
        {
            "name": "alpha",
            "database": "foo",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                    "amount": 7,
                },
                {
                    "name": "beta",
                    "unit": "kg",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                    "amount": 17,
                },
            ],
        }
    ]
    result = create_products_as_new_nodes(data)
    assert len(data) == 2
    assert result[1]["name"] == "beta"


def test_create_products_as_new_nodes_inherit_process_location():
    data = [
        {
            "name": "alpha",
            "database": "foo",
            "location": "here",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                }
            ],
        }
    ]
    result = create_products_as_new_nodes(data)
    assert len(data) == 2
    product = {
        "database": "foo",
        "code": result[1]["code"],
        "name": "beta",
        "unit": "kg",
        "location": "here",
        "exchanges": [],
        "type": bd.labels.product_node_default,
        "extra": True,
    }
    assert result[1] == product


def test_create_products_as_new_nodes_inherit_process_unit():
    data = [
        {
            "name": "alpha",
            "database": "foo",
            "unit": "kg",
            "exchanges": [
                {
                    "name": "beta",
                    "location": "here",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                }
            ],
        }
    ]
    result = create_products_as_new_nodes(data)
    assert len(data) == 2
    product = {
        "database": "foo",
        "code": result[1]["code"],
        "name": "beta",
        "unit": "kg",
        "location": "here",
        "exchanges": [],
        "type": bd.labels.product_node_default,
        "extra": True,
    }
    assert result[1] == product


def test_create_products_as_new_nodes_inherit_process_location_when_searching():
    data = [
        {
            "name": "beta",
            "location": "here",
        },
        {
            "name": "alpha",
            "database": "foo",
            "location": "here",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                }
            ],
        },
    ]
    create_products_as_new_nodes(data)
    assert len(data) == 2


def test_create_products_as_new_nodes_get_default_global_location():
    data = [
        {
            "name": "alpha",
            "database": "foo",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "functional": True,
                    "type": "technosphere",
                    "extra": True,
                }
            ],
        }
    ]
    result = create_products_as_new_nodes(data)
    assert len(data) == 2
    product = {
        "database": "foo",
        "code": result[1]["code"],
        "name": "beta",
        "unit": "kg",
        "location": bd.config.global_location,
        "exchanges": [],
        "type": bd.labels.product_node_default,
        "extra": True,
    }
    assert result[1] == product


def test_create_products_as_new_nodes_dataset_must_have_database_key():
    data = [
        {
            "name": "alpha",
            "exchanges": [
                {
                    "name": "beta",
                    "unit": "kg",
                    "functional": True,
                    "type": "technosphere",
                }
            ],
        }
    ]
    with pytest.raises(KeyError):
        create_products_as_new_nodes(data)
