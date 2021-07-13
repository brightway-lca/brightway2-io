# -*- coding: utf-8 -*-
from bw2data import Database, databases
from ..units import normalize_units as normalize_units_function
from ..errors import StrategyError
from ..utils import activity_hash, DEFAULT_FIELDS
from copy import deepcopy
import numbers
import numpy as np
import pprint


def format_nonunique_key_error(obj, fields, others):
    template = """Object in source database can't be uniquely linked to target database.\nProblematic dataset is:\n{ds}\nPossible targets include (at least one not shown):\n{targets}"""
    fields_to_print = list(fields or DEFAULT_FIELDS) + ["filename"]
    _ = lambda x: {field: x.get(field, "(missing)") for field in fields_to_print}
    return template.format(
        ds=pprint.pformat(_(obj)), targets=pprint.pformat([_(x) for x in others])
    )


def link_iterable_by_fields(
    unlinked, other=None, fields=None, kind=None, internal=False, relink=False
):
    """Generic function to link objects in ``unlinked`` to objects in ``other`` using fields ``fields``.

    The database to be linked must have uniqueness for each object for the given ``fields``.

    If ``kind``, limit objects in ``unlinked`` of type ``kind``.

    If ``relink``, link to objects which already have an ``input``. Otherwise, skip already linked objects.

    If ``internal``, linked ``unlinked`` to other objects in ``unlinked``. Each object must have the attributes ``database`` and ``code``."""
    if kind:
        kind = {kind} if isinstance(kind, str) else kind
        if relink:
            filter_func = lambda x: x.get("type") in kind
        else:
            filter_func = lambda x: x.get("type") in kind and not x.get("input")
    else:
        if relink:
            filter_func = lambda x: True
        else:
            filter_func = lambda x: not x.get("input")

    if internal:
        other = unlinked

    duplicates, candidates = {}, {}
    try:
        # Other can be a generator, so a bit convoluted
        for ds in other:
            key = activity_hash(ds, fields)
            if key in candidates:
                duplicates.setdefault(key, []).append(ds)
            else:
                candidates[key] = (ds["database"], ds["code"])
    except KeyError:
        raise StrategyError(
            "Not all datasets in database to be linked have "
            "``database`` or ``code`` attributes"
        )

    for container in unlinked:
        for obj in filter(filter_func, container.get("exchanges", [])):
            key = activity_hash(obj, fields)
            if key in duplicates:
                raise StrategyError(
                    format_nonunique_key_error(obj, fields, duplicates[key])
                )
            elif key in candidates:
                obj["input"] = candidates[key]
    return unlinked


def assign_only_product_as_production(db):
    """Assign only product as reference product.

    Skips datasets that already have a reference product or no production exchanges. Production exchanges must have a ``name`` and an amount.

    Will replace the following activity fields, if not already specified:

    * 'name' - name of reference product
    * 'unit' - unit of reference product
    * 'production amount' - amount of reference product

    """
    for ds in db:
        if ds.get("reference product"):
            continue
        products = [x for x in ds.get("exchanges", []) if x.get("type") == "production"]
        if len(products) == 1:
            product = products[0]
            assert product['name']
            ds['reference product'] = product.get('reference product',[]) or product['name']
            ds['production amount'] = product['amount']
            ds['name'] = ds.get('name') or product['name']
            ds['unit'] = ds.get('unit') or product.get('unit') or 'Unknown'
    return db


def link_technosphere_by_activity_hash(db, external_db_name=None, fields=None):
    """Link technosphere exchanges using ``activity_hash`` function.

    If ``external_db_name``, link against a different database; otherwise link internally.

    If ``fields``, link using only certain fields."""
    TECHNOSPHERE_TYPES = {"technosphere", "substitution", "production"}
    if external_db_name is not None:
        if external_db_name not in databases:
            raise StrategyError(
                "Can't find external database {}".format(external_db_name)
            )
        other = (
            obj
            for obj in Database(external_db_name)
            if obj.get("type", "process") == "process"
        )
        internal = False
    else:
        other = None
        internal = True
    return link_iterable_by_fields(
        db, other, internal=internal, kind=TECHNOSPHERE_TYPES, fields=fields
    )


def set_code_by_activity_hash(db, overwrite=False):
    """Use ``activity_hash`` to set dataset code.

    By default, won't overwrite existing codes, but will if ``overwrite`` is ``True``."""
    for ds in db:
        if "code" not in ds or overwrite:
            ds["code"] = activity_hash(ds)
    return db


def tupleize_categories(db):
    for ds in db:
        if ds.get("categories"):
            ds["categories"] = tuple(ds["categories"])
        for exc in ds.get("exchanges", []):
            if exc.get("categories"):
                exc["categories"] = tuple(exc["categories"])
    return db


def drop_unlinked(db):
    """This is the nuclear option - use at your own risk!"""
    for ds in db:
        ds["exchanges"] = [obj for obj in ds["exchanges"] if obj.get("input")]
    return db


def normalize_units(db):
    """Normalize units in datasets and their exchanges"""
    for ds in db:
        if "unit" in ds:
            ds["unit"] = normalize_units_function(ds["unit"])
        for exc in ds.get("exchanges", []):
            if "unit" in exc:
                exc["unit"] = normalize_units_function(exc["unit"])
        for param in ds.get("parameters", {}).values():
            if "unit" in param:
                param["unit"] = normalize_units_function(param["unit"])
    return db


def add_database_name(db, name):
    """Add database name to datasets"""
    for ds in db:
        ds["database"] = name
    return db


def convert_uncertainty_types_to_integers(db):
    """Generic number conversion function convert to floats. Return to integers."""
    for ds in db:
        for exc in ds["exchanges"]:
            try:
                exc["uncertainty type"] = int(exc["uncertainty type"])
            except:
                pass
    return db


def drop_falsey_uncertainty_fields_but_keep_zeros(db):
    """Drop fields like '' but keep zero and NaN.

    Note that this doesn't strip `False`, which behaves *exactly* like 0.

    """
    uncertainty_fields = [
        "minimum",
        "maximum",
        "scale",
        "shape",
        "loc",
    ]

    def drop_if_appropriate(exc):
        for field in uncertainty_fields:
            if field not in exc or exc[field] == 0:
                continue
            elif isinstance(exc[field], numbers.Number) and np.isnan(exc[field]):
                continue
            elif not exc[field]:
                del exc[field]

    for ds in db:
        for exc in ds["exchanges"]:
            drop_if_appropriate(exc)
    return db


def convert_activity_parameters_to_list(data):
    """Convert activity parameters from dictionary to list of dictionaries"""

    def _(key, value):
        dct = deepcopy(value)
        dct["name"] = key
        return dct

    for ds in data:
        if "parameters" in ds:
            ds["parameters"] = [_(x, y) for x, y in ds["parameters"].items()]

    return data


def split_exchanges(data, filter_params, changed_attributes, allocation_factors=None):
    """Split unlinked exchanges in ``data`` which satisfy ``filter_params`` into new exchanges with changed attributes.

    ``changed_attributes`` is a list of dictionaries with the attributes that should be changed.

    ``allocation_factors`` is an optional list of floats to allocate the original exchange amount to the respective copies defined in ``changed_attributes``. They don't have to sum to one. If ``allocation_factors`` are not defined, then exchanges are split equally.

    Resets uncertainty to ``UndefinedUncertainty`` (0).

    To use this function as a strategy, you will need to curry it first using ``functools.partial``.

    Example usage::

        split_exchanges(
            [
                {'exchanges': [{
                    'name': 'foo',
                    'location': 'bar',
                    'amount': 20
                }, {
                    'name': 'food',
                    'location': 'bar',
                    'amount': 12
                }]}
            ],
            {'name': 'foo'},
            [{'location': 'A'}, {'location': 'B', 'cat': 'dog'}
        ]
        >>> [
            {'exchanges': [{
                'name': 'food',
                'location': 'bar',
                'amount': 12
            }, {
                'name': 'foo',
                'location': 'A',
                'amount': 12.,
                'uncertainty_type': 0
            }, {
                'name': 'foo',
                'location': 'B',
                'amount': 8.,
                'uncertainty_type': 0,
                'cat': 'dog',
            }]}
        ]

    """
    if allocation_factors is None:
        allocation_factors = [1] * len(changed_attributes)

    total = sum(allocation_factors)

    if len(changed_attributes) != len(allocation_factors):
        raise ValueError("`changed_attributes` and `allocation_factors` must have same length")

    for ds in data:
        to_delete, to_add = [], []
        for index, exchange in enumerate(ds.get("exchanges", [])):
            if exchange.get('input'):
                continue
            if all(exchange.get(key) == value for key, value in filter_params.items()):
                to_delete.append(index)
                for factor, obj in zip(allocation_factors, changed_attributes):
                    exc = deepcopy(exchange)
                    exc['amount'] = exc['amount'] * factor / total
                    exc["uncertainty_type"] = 0
                    for key, value in obj.items():
                        exc[key] = value
                    to_add.append(exc)
        if to_delete:
            for index in to_delete[::-1]:
                del ds['exchanges'][index]
            ds['exchanges'].extend(to_add)
    return data
