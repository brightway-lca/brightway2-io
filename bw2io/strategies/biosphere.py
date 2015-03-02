from __future__ import print_function
from ..compatibility import ECOSPOLD_2_3_BIOSPHERE
from ..errors import StrategyError
from ..utils import activity_hash, load_json_data_file
from bw2data import databases, Database


def drop_unspecified_subcategories(db):
    """Drop subcategories if they are ``unspecified`` or ``(unspecified)``."""
    UNSPECIFIED = {'unspecified', '(unspecified)'}
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
    mapping = {tuple(x[:2]): x[2]
               for x in load_json_data_file("biosphere-2-3")}
    try:
        for ds in db:
            if ds.get('categories') and ds.get('type') == 'emission':
                ds[u'name'] = mapping.get(
                    (ds['categories'][0], ds['name']),
                    ds['name']
                )
            for exc in (exc for exc in ds.get('exchanges', [])
                        if exc.get('type') == 'biosphere'
                        and exc.get('categories')):
                exc[u'name'] = mapping.get(
                    (exc['categories'][0], exc['name']),
                    exc['name']
                )
    except KeyError:
        raise StrategyError(
            u"A dataset or exchange is missing the ``name`` attribute"
        )
    return db


def normalize_biosphere_categories(db):
    """Normalize biosphere categories to ecoinvent 3.1 standard"""
    for ds in db:
        if ds.get('categories') and ds.get('type') == 'emission':
            ds[u'categories'] = ECOSPOLD_2_3_BIOSPHERE.get(
                tuple(ds['categories']),
                ds['categories']
            )
        for exc in (exc for exc in ds.get('exchanges', [])
                    if exc.get('type') == 'biosphere'
                    and exc.get('categories')):
            exc[u'categories'] = ECOSPOLD_2_3_BIOSPHERE.get(
                tuple(exc['categories']),
                exc['categories']
            )
    return db


def link_biosphere_by_activity_hash(db, biosphere_db_name, force=False):
    """Link biosphere exchanges to ``emission`` datasets in database ``biosphere_db_name``.

    If ``force``, force linking even if a link already exists."""
    candidates = {activity_hash(obj): obj.key
                  for obj in Database(biosphere_db_name)
                  if obj.get('type') == 'emission'}
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc['type'] == 'biosphere' and (force or not exc.get('input')):
                try:
                    exc[u"input"] = candidates[activity_hash(exc)]
                except KeyError:
                    pass
    return db
