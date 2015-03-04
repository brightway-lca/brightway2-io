from __future__ import print_function
from ..compatibility import ECOSPOLD_2_3_BIOSPHERE
from ..errors import StrategyError
from ..utils import activity_hash, load_json_data_file
from .generic import link_iterable_by_fields
from bw2data import databases, Database


def drop_unspecified_subcategories(db):
    """Drop biosphere subcategories if they are in the following:
        * ``unspecified``
        * ``(unspecified)``
        * ``''``
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


def link_biosphere_by_activity_hash(db, biosphere_db_name, relink=False):
    """Link biosphere exchanges to ``emission`` datasets in database ``biosphere_db_name`` by matching activity hashes.

    If ``force``, force new linking even if a link already exists.

    Only links biosphere flows in data (i.e. ``flow = 'biosphere'``), and only against biosphere flows in ``biosphere_db_name``, (i.e. ``type = 'emission'``)."""
    if biosphere_db_name not in databases:
        raise StrategyError(u"Can't find external biosphere database {}".format(
                            biosphere_db_name))
    return link_iterable_by_fields(db, Database(biosphere_db_name), kind='emission', relink=relink)
