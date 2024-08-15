import math
import hashlib
import json
import os
import pprint
from numbers import Number

from stats_arrays import *

DEFAULT_FIELDS = ("name", "categories", "unit", "reference product", "location")


def activity_hash(data, fields=None, case_insensitive=True):
    """
    Hash an activity dataset.

    Used to import data formats like ecospold 1 (ecoinvent v1-2) and SimaPro, where no unique attributes for datasets are given.

    This is clearly an imperfect and brittle solution, but there is no other obvious approach at this time.

    By default, uses the following, in order:
    * name
    * categories
    * unit
    * reference product
    * location

    Parameters
    ----------
    data : dict
        The :ref:`activity dataset data <database-documents>`.


    fields : list, optional
        Optional list of fields to hash together. Default is ``('name', 'categories', 'unit', 'reference product', 'location')``.

        An empty string is used if a field isn't present. All fields are cast to lower case.


    case_insensitive : bool, optional
        Cast everything to lowercase before computing hash. Default is ``True``.

    Returns
    -------
    str
        A MD5 hash string, hex-encoded.

    """
    lower = lambda x: x.lower() if case_insensitive else x

    def get_value(obj, field):
        if isinstance(data.get(field), (list, tuple)):
            return lower("".join(data.get(field) or []))
        else:
            return lower(data.get(field) or "")

    fields = fields or DEFAULT_FIELDS
    string = "".join([get_value(data, field) for field in fields])
    return str(hashlib.md5(string.encode("utf-8")).hexdigest())


def es2_activity_hash(activity, flow):
    """
    Generate unique ID for ecoinvent3 dataset.

    Despite using a million UUIDs, there is actually no unique ID in an ecospold2 dataset.

    Datasets are uniquely identified by the combination of activity and flow UUIDs.

    Parameters
    ----------
    activity : str
        The activity UUID.
    flow : str
        The flow UUID.

    Returns
    -------
    str
        The unique ID.

    """
    return str(hashlib.md5((activity + flow).encode("utf-8")).hexdigest())


def load_json_data_file(filename):
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    if filename[-5:] != ".json":
        filename = filename + ".json"
    return json.load(open(os.path.join(DATA_DIR, filename), encoding="utf-8"))


def format_for_logging(obj):
    return pprint.pformat(obj, indent=2)


def rescale_exchange(exc: dict, factor: float) -> dict:
    """
    Rescale exchanges, including formulas and uncertainty values, by a constant factor.

    Parameters
    ----------
    exc : dict
        The exchange to rescale.
    factor : float
        The factor to rescale by.

    Returns
    -------
    dict
        The rescaled exchange.

    Raises
    ------
    ValueError
        If factor is not a number.

    """
    if not isinstance(factor, Number):
        raise ValueError(f"`factor` must be a number, but got {type(factor)}")

    if factor == 0:
        exc.update(
            {
                "uncertainty type": UndefinedUncertainty.id,
                "loc": exc['amount'] * factor,
                "amount": exc['amount'] * factor,
            }
        )
        for field in ("scale", "shape", "minimum", "maximum", "negative"):
            if field in exc:
                del exc[field]
    if exc.get("formula"):
        exc["formula"] = "({}) * {}".format(exc["formula"], factor)
    if exc.get("uncertainty type", 0) in (UndefinedUncertainty.id, NoUncertainty.id):
        exc["amount"] = exc["loc"] = factor * exc["amount"]
    elif exc["uncertainty type"] == NormalUncertainty.id:
        exc.update(
            {
                "scale": abs(exc["scale"] * factor),
                "loc": exc['amount'] * factor,
                "amount": exc['amount'] * factor,
            }
        )
    elif exc["uncertainty type"] == LognormalUncertainty.id:
        exc.update(
            {
                "loc": math.log(abs(exc['amount'] * factor)),
                "negative": (exc['amount'] * factor) < 0,
                "amount": exc['amount'] * factor,
            }
        )
    elif exc["uncertainty type"] in (TriangularUncertainty.id, UniformUncertainty.id):
        exc["minimum"] *= factor
        exc["maximum"] *= factor
        if "amount" in exc:
            exc["amount"] = exc["loc"] = factor * exc["amount"]
    else:
        raise UnsupportedExchange("This exchange type can't be automatically rescaled")

    for field in ("minimum", "maximum"):
        if field in exc:
            exc[field] *= factor
    if factor < 0 and "minimum" in exc and "maximum" in exc:
        exc["minimum"], exc["maximum"] = exc["maximum"], exc["minimum"]
    elif factor < 0 and "minimum" in exc:
        exc["maximum"] = exc["minimum"]
    elif factor < 0 and "maximum" in exc:
        exc["minimum"] = exc["maximum"]

    return exc


def standardize_method_to_len_3(name, padding="--", joiner=","):
    """
    Standardize an LCIA method name to a length 3 tuple.

    Parameters
    ----------
    name : tuple
        The current name.
    padding : str, optional
        The string to use for missing fields. The default is "--".
    joiner : str, optional
        The string to use to join the fields. The default is ",".

    Returns
    -------
    tuple
        The standardized name.
    """
    if len(name) >= 3:
        return tuple(name)[:2] + (joiner.join(name[2:]),)
    else:
        return (tuple(name) + (padding,) * 3)[:3]
