from copy import deepcopy

import bw2data as bd
import pytest

from bw2io.strategies import create_products_as_new_nodes, separate_processes_from_products


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


def test_separate_processes_from_products_basic():
    """Test basic functionality of separate_processes_from_products."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "location": "GLO",
            "unit": "kg",
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
                {
                    "input": ("test_db", "B001"),
                    "amount": 2,
                    "type": "technosphere",
                },
            ],
        },
        {
            "name": "Process B",
            "code": "B001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "location": "GLO",
            "unit": "kg",
            "exchanges": [
                {
                    "input": ("test_db", "B001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    # Should have 4 datasets: 2 original processes + 2 new products
    assert len(result) == 4

    # Original processes should now be process_node_default type
    assert result[0]["type"] == bd.labels.process_node_default
    assert result[1]["type"] == bd.labels.process_node_default

    # New products should be product_node_default type
    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 2

    # Check product attributes
    product_a = next(p for p in products if p["code"] == "A001-product")
    assert product_a["name"] == "Process A"
    assert product_a["code"] == "A001-product"
    assert product_a["database"] == "test_db"
    assert "location" not in product_a
    assert product_a["unit"] == "kg"
    assert product_a["exchanges"] == []

    # Check that exchanges are relinked
    process_a = result[0]
    production_exchange = next(e for e in process_a["exchanges"] if e.get("functional"))
    assert production_exchange["input"] == ("test_db", "A001-product")

    technosphere_exchange = next(e for e in process_a["exchanges"] if e["type"] == "technosphere")
    assert technosphere_exchange["input"] == ("test_db", "B001-product")


def test_separate_processes_from_products_with_reference_product():
    """Test that reference_product field is used as product name."""
    data = [
        {
            "name": "Process A",
            "reference_product": "Product Alpha",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1

    product = products[0]
    assert product["name"] == "Product Alpha"
    assert product["code"] == "A001-product"


def test_separate_processes_from_products_field_exclusions():
    """Test that excluded fields are not copied to products."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "location": "GLO",
            "unit": "kg",
            "extra_field": "should_be_excluded",
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data, field_exclusions=["location", "extra_field"])

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1

    product = products[0]
    assert "location" not in product
    assert "extra_field" not in product
    assert product["name"] == "Process A"
    assert product["unit"] == "kg"


def test_separate_processes_from_products_custom_code_suffix():
    """Test custom code suffix functionality."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data, code_suffix="_custom")

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1

    product = products[0]
    assert product["code"] == "A001_custom"


def test_separate_processes_from_products_skip_no_exchanges():
    """Test that processes without exchanges are skipped."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [],
        },
        {
            "name": "Process B",
            "code": "B001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "B001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    # Should have 3 datasets: 2 original processes + 1 new product (only for Process B)
    assert len(result) == 3

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1
    assert products[0]["code"] == "B001-product"


def test_separate_processes_from_products_skip_no_self_production():
    """Test that processes without self-referential production edges are skipped."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "B001"),
                    "amount": 1,
                    "type": "technosphere",
                },
            ],
        },
        {
            "name": "Process B",
            "code": "B001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "B001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    # Should have 3 datasets: 2 original processes + 1 new product (only for Process B)
    assert len(result) == 3

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1
    assert products[0]["code"] == "B001-product"


def test_separate_processes_from_products_multiple_self_production():
    """Test handling of multiple self-referential production edges."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
                {
                    "input": ("test_db", "A001"),
                    "amount": 0.5,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    # Should have 2 datasets: 1 original process + 1 new product
    assert len(result) == 2

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1

    # Both production exchanges should be relinked to the product
    process = result[0]
    production_exchanges = [e for e in process["exchanges"] if e.get("functional")]
    assert len(production_exchanges) == 2
    for exchange in production_exchanges:
        assert exchange["input"] == ("test_db", "A001-product")


def test_separate_processes_from_products_chimaera_nodes():
    """Test that chimaera nodes are also processed."""
    data = [
        {
            "name": "Chimaera Process",
            "code": "C001",
            "database": "test_db",
            "type": bd.labels.chimaera_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "C001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    # Should have 2 datasets: 1 original chimaera + 1 new product
    assert len(result) == 2

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1
    assert products[0]["code"] == "C001-product"


def test_separate_processes_from_products_existing_products_error():
    """Test that function raises error if product nodes already exist."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
        {
            "name": "Existing Product",
            "code": "P001",
            "database": "test_db",
            "type": bd.labels.product_node_default,
            "exchanges": [],
        },
    ]

    with pytest.raises(ValueError, match="This function requires no product nodes in the imported database"):
        separate_processes_from_products(data)


def test_separate_processes_from_products_code_overlap_error():
    """Test that function raises error if code suffix creates overlaps."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
        {
            "name": "Process B",
            "code": "A001-product",  # This would conflict with the suffix
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001-product"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    with pytest.raises(ValueError, match="Given `code_suffix` results in code overlaps"):
        separate_processes_from_products(data)


def test_separate_processes_from_products_complex_linking():
    """Test complex linking scenarios with multiple processes."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
                {
                    "input": ("test_db", "B001"),
                    "amount": 2,
                    "type": "technosphere",
                },
                {
                    "input": ("test_db", "C001"),
                    "amount": 3,
                    "type": "technosphere",
                },
            ],
        },
        {
            "name": "Process B",
            "code": "B001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "B001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": "technosphere",
                },
            ],
        },
        {
            "name": "Process C",
            "code": "C001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "exchanges": [
                {
                    "input": ("test_db", "C001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    # Should have 6 datasets: 3 original processes + 3 new products
    assert len(result) == 6

    # Check that all exchanges are properly relinked
    process_a = next(ds for ds in result if ds["code"] == "A001" and ds["type"] == bd.labels.process_node_default)
    process_b = next(ds for ds in result if ds["code"] == "B001" and ds["type"] == bd.labels.process_node_default)
    process_c = next(ds for ds in result if ds["code"] == "C001" and ds["type"] == bd.labels.process_node_default)

    # Check production exchanges
    prod_a = next(e for e in process_a["exchanges"] if e.get("functional"))
    prod_b = next(e for e in process_b["exchanges"] if e.get("functional"))
    prod_c = next(e for e in process_c["exchanges"] if e.get("functional"))

    assert prod_a["input"] == ("test_db", "A001-product")
    assert prod_b["input"] == ("test_db", "B001-product")
    assert prod_c["input"] == ("test_db", "C001-product")

    # Check technosphere exchanges
    assert next(e for e in process_a["exchanges"] if e["input"] == ("test_db", "B001-product"))
    assert next(e for e in process_a["exchanges"] if e["input"] == ("test_db", "C001-product"))
    assert next(e for e in process_b["exchanges"] if e["input"] == ("test_db", "A001-product"))



def test_separate_processes_from_products_preserve_metadata():
    """Test that metadata fields are properly preserved in products."""
    data = [
        {
            "name": "Process A",
            "code": "A001",
            "database": "test_db",
            "type": bd.labels.process_node_default,
            "location": "GLO",
            "unit": "kg",
            "comment": "Test process",
            "categories": ["test", "category"],
            "exchanges": [
                {
                    "input": ("test_db", "A001"),
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "functional": True,
                },
            ],
        },
    ]

    result = separate_processes_from_products(data)

    products = [ds for ds in result if ds["type"] == bd.labels.product_node_default]
    assert len(products) == 1

    product = products[0]
    assert product["name"] == "Process A"
    assert "location" not in product
    assert product["unit"] == "kg"
    assert product["comment"] == "Test process"
    assert product["categories"] == ["test", "category"]
    assert product["exchanges"] == []
