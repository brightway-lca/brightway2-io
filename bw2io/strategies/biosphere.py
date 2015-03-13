from __future__ import print_function
from ..compatibility import ECOSPOLD_2_3_BIOSPHERE
from ..errors import StrategyError
from ..utils import activity_hash
from .generic import link_iterable_by_fields
from .migrations import migrate_exchanges, migrate_datasets
from bw2data import databases, Database


def drop_unspecified_subcategories(db):
    """Drop biosphere subcategories if they are in the following:
        * ``unspecified``
        * ``(unspecified)``
        * ``''`` (empty string)
        * ``None``

    Only applies to biosphere processes (``type = 'emission'``) and biosphere exchanges (``type = 'biosphere'``).

    """
    UNSPECIFIED = {'unspecified', '(unspecified)', '', None}
    for ds in db:
        if (len(ds.get('categories', [])) == 2
                and ds['categories'][1] in UNSPECIFIED
                and ds.get('type') == 'emission'):
            ds[u'categories'] = (ds['categories'][0], )
        for exc in (exc for exc in ds.get('exchanges', [])
                    if exc.get('type') == 'biosphere'
                    and len(exc.get('categories', [])) == 2
                    and exc['categories'][1] in UNSPECIFIED):
            exc[u'categories'] = (exc['categories'][0],)
    return db


def normalize_biosphere_names(db):
    """Normalize biosphere flow names to ecoinvent 3.1 standard.

    Assumes that each dataset and each exchange have a ``name``. Will change names even if exchange is already linked."""
    db = migrate_exchanges(db, migration="biosphere-2-3-names")
    db = migrate_datasets(db, migration="biosphere-2-3-names")
    return db


def normalize_biosphere_categories(db):
    """Normalize biosphere categories to ecoinvent 3.1 standard"""
    db = migrate_exchanges(db, migration="biosphere-2-3-categories")
    db = migrate_datasets(db, migration="biosphere-2-3-categories")
    return db


def strip_biosphere_exc_locations(db):
    """Biosphere flows don't have locations - if any are included they can confuse linking"""
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc.get('type') == 'biosphere' and 'location' in exc:
                del exc['location']
    return db
