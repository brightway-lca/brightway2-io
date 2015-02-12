# -*- coding: utf-8 -*
from __future__ import division, print_function
from ..utils import activity_hash
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


def link_based_on_name_and_unit(db):
    """Create internal links in database based on unique names and unit"""
    db_name = {ds['database'] for ds in db}
    assert len(db_name) == 1
    name_dict = {
        (ds['name'], ds['unit']): (ds['database'], ds['code'])
        for ds in db
    }
    for ds in db:
        for exc in ds.get('exchanges', []):
            if (exc['name'], exc['unit']) in name_dict and not exc.get("input"):
                exc[u'input'] = name_dict[(exc['name'], exc['unit'])]
    return db


def split_simapro_name_geo(db):
    """Split a name like 'foo/CH U' into name and geo components"""
    for ds in db:
        found = detoxify_re.findall(ds['name'])
        if found:
            ds[u'location'] = found[0][0]
            ds[u'name'] = ds[u"reference product"] = \
                re.sub(detoxify_pattern, '', ds['name'])
    return db


def sp_detoxify_link_external_technosphere_by_activity_hash(db, external_db_name):
    def reformat(ds):
        """SimaPro doesn't include categories"""
        return {
            'name': ds['name'],
            'unit': ds['unit'],
            'location': ds['location']
        }

    assert external_db_name in databases, \
        u"Unknown database {}".format(external_db_name)
    TECHNOSPHERE_TYPES = {u"technosphere", u"substitution", u"production"}
    print("Loading background database: {}".format(external_db_name))
    candidates = {activity_hash(reformat(ds)): ds.key
                  for ds in Database(external_db_name)}
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc.get('type') in TECHNOSPHERE_TYPES and not exc.get("input"):
                try:
                    exc2 = copy.deepcopy(exc)
                    name, location, _ = detoxify_re.findall(exc2['name'])[0]
                    exc2['name'], exc2['location'] = name, location
                    exc[u'input'] = candidates[activity_hash(reformat(exc2))]
                    if 'unlinked' in exc:
                        del exc['unlinked']
                except (KeyError, IndexError):
                    continue
    return db
