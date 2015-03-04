# -*- coding: utf-8 -*
from __future__ import division, print_function
from ..compatibility import (
    SIMAPRO_BIO_SUBCATEGORIES,
    SIMAPRO_BIOSPHERE,
    SIMAPRO_SYSTEM_MODELS,
)
from ..errors import StrategyError
from .generic import link_iterable_by_fields
from ..utils import activity_hash, load_json_data_file
from bw2data import databases, Database
import copy
import re


# Pattern for SimaPro munging of ecoinvent names
detoxify_pattern = '^(?P<name>.+?)/(?P<geo>[A-Za-z]{2,10})(/I)? [SU]$'
detoxify_re = re.compile(detoxify_pattern)


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


def link_based_on_name_unit_location(db):
    """Create internal technosphere links based on name, unit, and location. Can't use categories because we can't reliably extract categories from datasets, only exchanges."""
    fields = ('name', 'location', 'unit')
    return link_iterable_by_fields(db, None, fields=fields, internal=True, kind='technosphere')


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
            subcat = SIMAPRO_BIO_SUBCATEGORIES.get(
                exc['categories'][1],
                exc['categories'][1]
            )
            exc[u'categories'] = (cat, subcat)
    return db


def normalize_simapro_lcia_biosphere_categories(data):
    """Normalize categories to ecoinvent standard"""
    for method in data:
        for cf in method['exchanges']:
            cat = SIMAPRO_BIOSPHERE.get(
                cf['categories'][0],
                cf['categories'][0]
            )
            if len(cf['categories']) > 1:
                subcat = SIMAPRO_BIO_SUBCATEGORIES.get(
                    cf['categories'][1],
                    cf['categories'][1]
                )
                cf[u'categories'] = (cat, subcat)
            else:
                cf[u'categories'] = (cat,)
    return data


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


def sp_match_ecoinvent3_database(db, ei3_name, system_model, debug=False):
    """Link SimaPro transformed names to an ecoinvent 3.X database.

    Will temporarily load database ``db_name`` into memory.

    If ``system_model``, will only link processes from the given system model. Available ``system_model``s are:

        * apos
        * consequential
        * cutoff

    Correspondence file is from Pré, and has the following fields:

        #. SimaPro name
        #. Ecoinvent flow name
        #. Location
        #. Ecoinvent activity name
        #. System model
        #. SimaPro type

    Note that even the official matching data from Pré is incorrect, so we have to cast all strings to lower case.

    SimaPro type is either ``System terminated`` or ``Unit process``. We always match to unit processes regardless of SimaPro type.

    """
    assert db_name in databases, u"Unknown database {}".format(db_name)
    try:
        system_model = SIMAPRO_SYSTEM_MODELS[system_model]
    except KeyError:
        raise ValueError(u"``system_model`` must be one of: cutoff, "
            u"consequential, apos")

    to_lower = lambda x, y, z: (x.lower(), y.lower(), z.lower())
    matching_data = load_json_data_file("simapro-ecoinvent31")

    print(u"Loading background database: {}".format(db_name))
    possibles = {to_lower(obj['reference product'], obj.location, obj.name): obj.key for obj in Database(db_name)}
    sp_mapping = {line[0]: possibles.get(to_lower(*line[1:4]))
        for line in matching_data
        if line[4] == system_model}

    print(u"Matching exchanges")
    for ds in self.data:
        for exc in ds.get('exchanges', []):
            if exc.get('input') or exc['name'] not in sp_mapping:
                continue
            else:
                exc[u'input'] = sp_mapping[exc['name']]
                if 'unlinked' in exc:
                    del exc['unlinked']
    if debug:
        return possibles, matching_data, sp_mapping
    else:
        return db
