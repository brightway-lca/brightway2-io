# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals, division
from eight import *

from ..compatibility import (
    SIMAPRO_BIO_SUBCATEGORIES,
    SIMAPRO_BIOSPHERE,
    SIMAPRO_SYSTEM_MODELS,
)
from ..errors import StrategyError
from .generic import (
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
)
from ..utils import activity_hash, load_json_data_file
from ..units import normalize_units
from bw2data import databases, Database
import copy
import re


# Pattern for SimaPro munging of ecoinvent names
detoxify_pattern = '^(?P<name>.+?)/(?P<geo>[A-Za-z]{2,10})(/I)? [SU]$'
detoxify_re = re.compile(detoxify_pattern)


def normalize_simapro_product_units(db):
    """Normalize SimaPro product units.

    SimaPro products are defined separately, so units need to be normalized separately."""
    for ds in db:
        for product in ds.get('products', []):
            if 'unit' in product:
                product['unit'] = normalize_units(product['unit'])
    return db


def sp_allocate_products(db):
    """Create a dataset from each product in a raw SimaPro dataset"""
    new_db = []
    for ds in db:
        if ds.get("reference product"):
            new_db.append(ds)
        elif not ds['products']:
            ds["error"] = True
            new_db.append(ds)
        elif len(ds['products']) == 1:
            # Waste treatment datasets only allowed one product
            product = ds['products'][0]
            ds[u'name'] = ds[u'reference product'] = product['name']
            ds[u'unit'] = product['unit']
            ds[u'production amount'] = product['amount']
            new_db.append(ds)
        else:
            ds[u'exchanges'] = [exc for exc in ds['exchanges']
                                if exc['type'] != "production"]
            for product in ds['products']:
                product = copy.deepcopy(product)
                if product['allocation']:
                    product[u'amount'] = (product['amount'] *
                        1 / (product['allocation'] / 100))
                else:
                    product[u'amount'] = 0
                copied = copy.deepcopy(ds)
                copied[u'exchanges'].append(product)
                copied[u'products'] = [product]
                copied[u'name'] = copied[u'reference product'] = product['name']
                copied[u'unit'] = product['unit']
                copied[u'production amount'] = product['amount']
                new_db.append(copied)
    return new_db


def link_technosphere_based_on_name_unit_location(db, external_db_name=None):
    """Link technosphere exchanges based on name, unit, and location. Can't use categories because we can't reliably extract categories from SimaPro exports, only exchanges.

    If ``external_db_name``, link against a different database; otherwise link internally."""
    return link_technosphere_by_activity_hash(db,
        external_db_name=external_db_name,
        fields=('name', 'location', 'unit')
    )

def split_simapro_name_geo(db):
    """Split a name like 'foo/CH U' into name and geo components.

    Sets original name to ``simapro name``."""
    for ds in db:
        match = detoxify_re.match(ds['name'])
        if match:
            gd = match.groupdict()
            ds[u'simapro name'] = ds['name']
            ds[u'location'] = gd['geo']
            ds[u'name'] = ds[u"reference product"] = gd['name']
        for exc in ds.get('exchanges', []):
            match = detoxify_re.match(exc['name'])
            if match:
                gd = match.groupdict()
                exc[u'simapro name'] = exc['name']
                exc[u'location'] = gd['geo']
                exc[u'name'] = gd['name']
    return db


def normalize_simapro_biosphere_categories(db):
    """Normalize biosphere categories to ecoinvent standard."""
    for ds in db:
        for exc in (exc for exc in ds.get('exchanges', [])
                    if exc['type'] == 'biosphere'):
            cat = SIMAPRO_BIOSPHERE.get(
                exc['categories'][0],
                exc['categories'][0]
            )
            if len(exc['categories']) > 1:
                subcat = SIMAPRO_BIO_SUBCATEGORIES.get(
                    exc['categories'][1],
                    exc['categories'][1]
                )
                exc[u'categories'] = (cat, subcat)
            else:
                exc[u'categories'] = (cat, )
    return db


def normalize_simapro_biosphere_names(db):
    """Normalize biosphere flow names to ecoinvent standard"""
    mapping = {tuple(x[:2]): x[2]
               for x in load_json_data_file("simapro-biosphere")}
    for ds in db:
        for exc in (exc for exc in ds.get('exchanges', [])
                    if exc['type'] == 'biosphere'):
            try:
                exc['name'] = mapping[(exc['categories'][0], exc['name'])]
            except KeyError:
                pass
    return db


def normalize_simapro_formulae(formula, settings):
    """Convert SimaPro formulae to Python"""
    def replace_comma(match):
        return match.group(0).replace(",", ".")

    formula = formula.replace("^", "**")
    if settings.get('Decimal separator') == ',':
        formula = re.sub('\d,\d', replace_comma, formula)
    return formula
