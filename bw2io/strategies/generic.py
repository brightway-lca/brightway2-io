from bw2data import mapping, Database
from ..utils import activity_hash
from ..errors import StrategyError


def link_iterable_by_fields(unlinked, other, fields=None, kind=None,
                            internal=False, relink=False):
    """Generic function to link objects in ``unlinked`` to objects in ``other`` using fields ``fields``.

    The database to be linked must have uniqueness for each object for the given ``fields``.

    If ``kind``, limit objects in ``unlinked`` of type ``kind``.

    If ``relink``, link to objects which already have an ``input``. Otherwise, skip already linked objects.

    If ``internal``, linked ``unlinked`` to other objects in ``unlinked``. Each object must have the attributes ``database`` and ``code``."""
    if kind:
        kind = {kind} if isinstance(kind, basestring) else kind
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

    try:
        candidates = {
            activity_hash(ds, fields): (ds['database'], ds['code'])
            for ds in other
        }
    except KeyError:
        raise StrategyError(u"Not all datasets in database to be linked have "
                            u"``database`` or ``code`` attributes")

    if len(candidates) != len(other):
        raise StrategyError(u"Not each object in database to be linked is "
                            u"unique with given fields")

    for container in unlinked:
        for obj in filter(filter_func, container.get('exchanges', [])):
            try:
                obj[u'input'] = candidates[activity_hash(obj, fields)]
                if obj.get('unlinked'):
                    del obj['unlinked']
            except KeyError:
                pass
    return unlinked


def assign_only_product_as_production(db):
    """Assign only product as reference product"""
    for ds in db:
        if ds.get("reference product"):
            continue
        if len(ds['products']) == 1:
            ds[u'name'] = ds['products'][0]['name']
            ds[u'unit'] = ds['products'][0]['unit']
            ds[u'production amount'] = ds['products'][0]['amount']
    return db


def link_internal_technosphere_by_activity_hash(db):
    TECHNOSPHERE_TYPES = {u"technosphere", u"substitution", u"production"}
    return link_iterable_by_fields(db, None, internal=True, kind=TECHNOSPHERE_TYPES)


def link_external_technosphere_by_activity_hash(db, external_db_name):
    if external_db_name not in databases:
        raise StrategyError(u"Can't find external database {}".format(
                            external_db_name))
    TECHNOSPHERE_TYPES = {u"technosphere", u"substitution", u"production"}
    return link_iterable_by_fields(db, Database(external_db_name), kind=TECHNOSPHERE_TYPES)


def set_code_by_activity_hash(db):
    """Use ``activity_hash`` to set dataset code"""
    for ds in db:
        ds[u'code'] = activity_hash(ds)
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
            elif exc.get('unlinked'):
                del exc['unlinked']
    return db
