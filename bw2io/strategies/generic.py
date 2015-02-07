from bw2data import mapping
from ..utils import activity_hash


def assign_only_product_as_reference_product(db):
    """Assign only product as reference product"""
    for ds in db:
        if len(ds['products']) == 1:
            ds[u'name'] = ds[u'reference product'] = ds['products'][0]['name']
            ds[u'unit'] = ds['products'][0]['unit']
            ds[u'production amount'] = ds['products'][0]['amount']
    return db


def link_biosphere_by_activity_hash(db, biosphere=u"biosphere"):
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc.get('biosphere') and not exc.get("input"):
                key = (biosphere, activity_hash(exc))
                if key in mapping:
                    exc[u"input"] = key
    return db


def link_internal_technosphere_by_activity_hash(db):
    TECHNOSPHERE_TYPES = {u"technosphere", u"substitution", u"production"}
    db_name = {ds['database'] for ds in db}
    assert len(db_name) == 1
    candidates = {(db_name, activity_hash(ds)) for ds in db}
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc.get('type') in TECHNOSPHERE_TYPES and not exc.get("input"):
                key = (db_name, activity_hash(exc))
                if key in candidates:
                    exc[u"input"] = key
    return db


def assign_only_production_with_amount_as_reference_product(db):
    """If a multioutput process has one product with a non-zero amount, assign that product as reference product"""
    for ds in db:
        amounted = [prod for prod in ds['products'] if prod['amount']]
        if len(amounted) == 1:
            ds[u'name'] = ds[u'reference product'] = amounted[0]['name']
            ds[u'unit'] = amounted[0]['unit']
            ds[u'production amount'] = amounted[0]['amount']
    return db


def mark_unlinked_exchanges(db):
    """Set ``unlinked`` flag for exchanges without an ``input``"""
    for ds in db:
        for exc in ds.get('exchanges', []):
            if not exc.get('input'):
                exc[u"unlinked"] = True
    return db
