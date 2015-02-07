# -*- coding: utf-8 -*
import re

# Pattern for SimaPro munging of ecoinvent names
detoxify_pattern = '/(?P<geo>[A-Z]{2,10})(/I)? [SU]$'
detoxify_re = re.compile(detoxify_pattern)


def assign_100_percent_allocation_as_reference_product(db):
    """If a multioutput process has one product with 100%% allocation, assign that product as reference product"""
    for ds in db:
        allocated = [prod for prod in ds['products'] if prod['allocation'] == 100]
        if len(allocated) == 1:
            ds[u'name'] = ds[u'reference product'] = allocated[0]['name']
            ds[u'unit'] = allocated[0]['unit']
            ds[u'production amount'] = allocated[0]['amount']
    return db


def link_based_on_name(db):
    """Create internal links in database based on unique names"""
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
