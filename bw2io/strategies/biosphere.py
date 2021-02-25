# -*- coding: utf-8 -*-
from .migrations import migrate_exchanges, migrate_datasets


def drop_unspecified_subcategories(db):
    """Drop subcategories if they are in the following:
        * ``unspecified``
        * ``(unspecified)``
        * ``''`` (empty string)
        * ``None``

    """
    UNSPECIFIED = {"unspecified", "(unspecified)", "", None}
    for ds in db:
        if ds.get("categories"):
            while ds["categories"] and ds["categories"][-1] in UNSPECIFIED:
                ds["categories"] = ds["categories"][:-1]
        for exc in ds.get("exchanges", []):
            if exc.get("categories"):
                while exc["categories"] and exc["categories"][-1] in UNSPECIFIED:
                    exc["categories"] = exc["categories"][:-1]
    return db


def normalize_biosphere_names(db, lcia=False):
    """Normalize biosphere flow names to ecoinvent 3.1 standard.

    Assumes that each dataset and each exchange have a ``name``. Will change names even if exchange is already linked."""
    db = migrate_exchanges(db, migration="biosphere-2-3-names")
    if not lcia:
        db = migrate_datasets(db, migration="biosphere-2-3-names")
    return db


def normalize_biosphere_categories(db, lcia=False):
    """Normalize biosphere categories to ecoinvent 3.1 standard"""
    db = migrate_exchanges(db, migration="biosphere-2-3-categories")
    if not lcia:
        db = migrate_datasets(db, migration="biosphere-2-3-categories")
    return db


def strip_biosphere_exc_locations(db):
    """Biosphere flows don't have locations - if any are included they can confuse linking"""
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc.get("type") == "biosphere" and "location" in exc:
                del exc["location"]
    return db


def ensure_categories_are_tuples(db):
    for ds in db:
        if ds.get("categories") and type(ds["categories"]) != tuple:
            ds["categories"] = tuple(ds["categories"])
    return db
