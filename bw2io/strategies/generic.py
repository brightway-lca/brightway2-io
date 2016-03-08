# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import mapping, Database, databases
from ..units import normalize_units as normalize_units_function
from ..utils import activity_hash
from ..errors import StrategyError
import pprint


def link_iterable_by_fields(unlinked, other=None, fields=None, kind=None,
                            internal=False, relink=False):
    """Generic function to link objects in ``unlinked`` to objects in ``other`` using fields ``fields``.

    The database to be linked must have uniqueness for each object for the given ``fields``.

    If ``kind``, limit objects in ``unlinked`` of type ``kind``.

    If ``relink``, link to objects which already have an ``input``. Otherwise, skip already linked objects.

    If ``internal``, linked ``unlinked`` to other objects in ``unlinked``. Each object must have the attributes ``database`` and ``code``."""
    if kind:
        kind = {kind} if isinstance(kind, str) else kind
        if relink:
            filter_func = lambda x: x.get('type') in kind
        else:
            filter_func = lambda x: x.get('type') in kind and not x.get('input')
    else:
        if relink:
            filter_func = lambda x: True
        else:
            filter_func = lambda x: not x.get('input')

    if internal:
        other = unlinked

    # Perhaps slightly convoluted, but other can be a generator
    duplicates = {}
    candidates = {}
    try:
        for ds in other:
            key = activity_hash(ds, fields)
            if key in candidates:
                duplicates[key] = ds
            else:
                candidates[key] = (ds['database'], ds['code'])
    except KeyError:
        raise StrategyError("Not all datasets in database to be linked have "
                            "``database`` or ``code`` attributes")

    if duplicates:
        raise StrategyError("Not each object in database to be linked is "
                            "unique with given fields. The following appear "
                            "at least twice:\n{}".format(pprint.pformat(
                                list(duplicates.values())))
                            )

    for container in unlinked:
        for obj in filter(filter_func, container.get('exchanges', [])):
            try:
                obj['input'] = candidates[activity_hash(obj, fields)]
            except KeyError:
                pass
    return unlinked


def assign_only_product_as_production(db):
    """Assign only product as reference product.

    Skips datasets that already have a reference product.

    This requires something to extract production exchanges to a new list called ``products``. Usually this happens in the extractors, but it could also be a strategy."""
    for ds in db:
        if ds.get("reference product"):
            continue
        products = [x for x in ds.get('exchanges', []) if x.get('type') == 'production']
        if len(products) == 1:
            product = products[0]
            ds['name'] = product['name']
            ds['unit'] = product.get('unit') or 'Unknown'
            ds['production amount'] = product['amount']
    return db


def link_technosphere_by_activity_hash(db, external_db_name=None, fields=None):
    """Link technosphere exchanges using ``activity_hash`` function.

    If ``external_db_name``, link against a different database; otherwise link internally.

    If ``fields``, link using only certain fields."""
    TECHNOSPHERE_TYPES = {"technosphere", "substitution", "production"}
    if external_db_name is not None:
        if external_db_name not in databases:
            raise StrategyError("Can't find external database {}".format(
                                external_db_name))
        other = (obj for obj in Database(external_db_name)
                 if obj.get('type', 'process') == 'process')
        internal = False
    else:
        other = None
        internal = True
    return link_iterable_by_fields(db, other, internal=internal, kind=TECHNOSPHERE_TYPES, fields=fields)


def set_code_by_activity_hash(db):
    """Use ``activity_hash`` to set dataset code"""
    for ds in db:
        ds['code'] = activity_hash(ds)
    return db


def tupleize_categories(db):
    for ds in db:
        if ds.get('categories'):
            ds['categories'] = tuple(ds['categories'])
        for exc in ds.get('exchanges', []):
            if exc.get('categories'):
                exc['categories'] = tuple(exc['categories'])
    return db


def drop_unlinked(db):
    """This is the nuclear option - use at your own risk!"""
    for ds in db:
        ds['exchanges'] = [obj for obj in ds['exchanges'] if obj.get('input')]
    return db


def normalize_units(db):
    """Normalize units in datasets and their exchanges"""
    for ds in db:
        if 'unit' in ds:
            ds['unit'] = normalize_units_function(ds['unit'])
        for exc in ds.get('exchanges', []):
            if 'unit' in exc:
                exc['unit'] = normalize_units_function(exc['unit'])
        for param in ds.get('parameters', {}).values():
            if 'unit' in param:
                param['unit'] = normalize_units_function(param['unit'])
    return db


def add_database_name(db, name):
    """Add database name to datasets"""
    for ds in db:
        ds['database'] = name
    return db
