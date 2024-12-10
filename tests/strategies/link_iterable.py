import copy
import unittest

import pytest

from bw2io.errors import StrategyError
from bw2io.strategies import link_iterable_by_fields


def test_all_datasets_in_target_have_database_field():
    assert link_iterable_by_fields([], [{"database": "foo", "code": "bar"}]) == []
    with pytest.raises(StrategyError):
        link_iterable_by_fields([], [{"code": "bar"}])


def test_all_datasets_in_target_have_code_field():
    assert link_iterable_by_fields([], [{"database": "foo", "code": "bar"}]) == []
    with pytest.raises(StrategyError):
        link_iterable_by_fields([], [{"database": "foo"}])


def test_nonunique_target_but_not_linked_no_error():
    data = [
        {"name": "foo", "database": "a", "code": "b"},
        {"name": "foo", "database": "a", "code": "c"},
        {"name": "bar", "database": "a", "code": "d"},
    ]
    assert link_iterable_by_fields([{"exchanges": [{"name": "bar"}]}], data) == [
        {"exchanges": [{"name": "bar", "input": ("a", "d")}]}
    ]


def test_nonunique_target_raises_error():
    data = [
        {"name": "foo", "database": "a", "code": "b"},
        {"name": "foo", "database": "a", "code": "c"},
        {"name": "bar", "database": "a", "code": "d"},
    ]
    with pytest.raises(StrategyError):
        link_iterable_by_fields([{"exchanges": [{"name": "foo"}]}], data)


def test_generic_linking_no_kind_no_relink():
    unlinked = [
        {
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                },
                {
                    "name": "foo",
                    "unit": "kilogram",
                    "type": "b",
                },
            ]
        }
    ]
    other = [
        {"name": "foo", "categories": ("bar",), "database": "db", "code": "first"},
        {"name": "baz", "categories": ("bar",), "database": "db", "code": "second"},
    ]
    expected = [
        {
            "exchanges": [
                {
                    "name": "foo",
                    "type": "a",
                    "categories": ("bar",),
                    "input": ("db", "first"),
                },
                {"name": "foo", "type": "b", "unit": "kilogram"},
            ]
        }
    ]
    assert link_iterable_by_fields(unlinked, other) == expected


def test_internal_linking():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {"name": "foo", "categories": ("bar",)},
                {"name": "foo", "categories": ("baz",)},
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "input": ("db", "first"),
                },
                {
                    "name": "foo",
                    "categories": ("baz",),
                    "input": ("db", "second"),
                },
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    assert link_iterable_by_fields(unlinked, internal=True) == expected


def test_edge_kinds_filter():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                },
                {"name": "foo", "categories": ("baz",)},
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                    "input": ("db", "first"),
                },
                {
                    "name": "foo",
                    "categories": ("baz",),
                },
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    assert (
        link_iterable_by_fields(unlinked, internal=True, edge_kinds=["a"]) == expected
    )
    assert link_iterable_by_fields(unlinked, internal=True, edge_kinds="a") == expected


def test_edge_kinds_filter_and_relink():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                    "input": ("something", "else"),
                },
                {"name": "foo", "categories": ("baz",)},
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                    "input": ("db", "first"),
                },
                {
                    "name": "foo",
                    "categories": ("baz",),
                },
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    assert (
        link_iterable_by_fields(unlinked, internal=True, edge_kinds=["a"], relink=True)
        == expected
    )


def test_relink():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                    "input": ("something", "else"),
                },
                {
                    "name": "foo",
                    "type": "b",
                    "input": ("something", "else"),
                    "categories": ("baz",),
                },
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                    "input": ("db", "first"),
                },
                {
                    "name": "foo",
                    "type": "b",
                    "input": ("db", "second"),
                    "categories": ("baz",),
                },
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "foo",
            "categories": ("baz",),
            "exchanges": [],
        },
    ]
    assert link_iterable_by_fields(unlinked, internal=True, relink=True) == expected


def test_linking_with_fields():
    unlinked = [
        {
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "type": "a",
                },
                {
                    "name": "foo",
                    "categories": ("baz",),
                    "unit": "kilogram",
                    "type": "b",
                },
            ]
        }
    ]
    other = [
        {"name": "foo", "categories": ("bar",), "database": "db", "code": "first"},
        {"name": "foo", "categories": ("baz",), "database": "db", "code": "second"},
    ]
    expected = [
        {
            "exchanges": [
                {
                    "name": "foo",
                    "type": "a",
                    "categories": ("bar",),
                    "input": ("db", "first"),
                },
                {
                    "name": "foo",
                    "type": "b",
                    "categories": ("baz",),
                    "input": ("db", "second"),
                    "unit": "kilogram",
                },
            ]
        }
    ]
    assert (
        link_iterable_by_fields(unlinked, other, fields=["name", "categories"])
        == expected
    )


def test_no_relink_skips_linking():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "input": ("something", "else"),
                }
            ],
        }
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "categories": ("bar",),
            "exchanges": [
                {
                    "name": "foo",
                    "categories": ("bar",),
                    "input": ("db", "first"),
                }
            ],
        }
    ]
    assert link_iterable_by_fields(copy.deepcopy(unlinked), internal=True) == unlinked

    del unlinked[0]["exchanges"][0]["input"]
    assert link_iterable_by_fields(unlinked, internal=True) == expected


def test_node_filters():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "type": "process",
            "exchanges": [
                {
                    "name": "bar",
                }
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "bar",
            "type": "product",
            "exchanges": [
                {
                    "name": "foo",
                }
            ],
        },
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "type": "process",
            "exchanges": [{"name": "bar", "input": ("db", "second")}],
        },
        {
            "database": "db",
            "code": "second",
            "name": "bar",
            "type": "product",
            "exchanges": [
                {
                    "name": "foo",
                }
            ],
        },
    ]

    assert (
        link_iterable_by_fields(
            unlinked,
            this_node_kinds=["process"],
            other_node_kinds=["product"],
            internal=True,
        )
        == expected
    )


def test_without_node_filters():
    unlinked = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "type": "process",
            "exchanges": [
                {
                    "name": "bar",
                }
            ],
        },
        {
            "database": "db",
            "code": "second",
            "name": "bar",
            "type": "product",
            "exchanges": [
                {
                    "name": "foo",
                }
            ],
        },
    ]
    expected = [
        {
            "database": "db",
            "code": "first",
            "name": "foo",
            "type": "process",
            "exchanges": [{"name": "bar", "input": ("db", "second")}],
        },
        {
            "database": "db",
            "code": "second",
            "name": "bar",
            "type": "product",
            "exchanges": [{"name": "foo", "input": ("db", "first")}],
        },
    ]

    assert link_iterable_by_fields(unlinked, internal=True) == expected
