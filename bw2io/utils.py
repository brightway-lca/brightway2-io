# -*- coding: utf-8 -*-
from numbers import Number
from stats_arrays import *
import hashlib
import json
import os
import pprint
import re
from bw2data import get_activity

from .errors import StrategyError, UnsupportedExchange

DEFAULT_FIELDS = ("name", "categories", "unit", "reference product", "location")


def es2_activity_hash(activity, flow):
    """Generate unique ID for ecoinvent3 dataset.

    Despite using a million UUIDs, there is actually no unique ID in an ecospold2 dataset. Datasets are uniquely identified by the combination of activity and flow UUIDs."""
    return str(hashlib.md5((activity + flow).encode("utf-8")).hexdigest())


def load_json_data_file(filename):
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    if filename[-5:] != ".json":
        filename = filename + ".json"
    return json.load(open(os.path.join(DATA_DIR, filename), encoding="utf-8"))


def format_for_logging(obj):
    return pprint.pformat(obj, indent=2)


def rescale_exchange(exc, factor):
    """Rescale exchanges, including formulas and uncertainty values, by a constant factor.

    No generally recommended, but needed for use in unit conversions. Not well tested.

    """
    assert isinstance(factor, Number)
    assert factor > 0 or exc.get("uncertainty type", 0) in {
        UndefinedUncertainty.id,
        NoUncertainty.id,
        NormalUncertainty.id,
    }
    if exc.get("formula"):
        exc["formula"] = "({}) * {}".format(exc["formula"], factor)
    if exc.get("uncertainty type", 0) in (UndefinedUncertainty.id, NoUncertainty.id):
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
    elif exc["uncertainty type"] == NormalUncertainty.id:
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
        exc[u"scale"] *= factor
    elif exc["uncertainty type"] == LognormalUncertainty.id:
        # ``scale`` in lognormal is scale-independent
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
    elif exc["uncertainty type"] == TriangularUncertainty.id:
        exc[u"minimum"] *= factor
        exc[u"maximum"] *= factor
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
    elif exc["uncertainty type"] == UniformUncertainty.id:
        exc[u"minimum"] *= factor
        exc[u"maximum"] *= factor
        if "amount" in exc:
            exc[u"amount"] *= factor
    else:
        raise UnsupportedExchange(u"This exchange type can't be automatically rescaled")
    return exc


def standardize_method_to_len_3(name, padding="--", joiner=","):
    """Standardize an LCIA method name to a length 3 tuple.

    ``name`` is the current name.
    ``padding`` is the string to use for missing fields.

    """
    if len(name) >= 3:
        return tuple(name)[:2] + (joiner.join(name[2:]),)
    else:
        return (tuple(name) + (padding,) * 3)[:3]


class ExchangeLinker:
    re_sub = re.compile(r"[()\[\],'\"]")

    field_funcs = {"default": lambda act, field: act.get(field, "")}

    @staticmethod
    def parse_field(
            field_value,
            case_insensitive=True,
            strip=True,
            re_sub=re_sub,
    ):
        if field_value is None:
            return None
        else:
            value = str(field_value)
            if case_insensitive:
                value = value.lower()
            if strip:
                value = value.strip()
            if re_sub is not None:
                value = re_sub.sub("", value)
            return value

    @staticmethod
    def format_nonunique_key_error(obj, fields, others):
        template = """Object in source database can't be uniquely linked to target database.\nProblematic dataset \
        is:\n{ds}\nPossible targets include (at least one not shown):\n{targets}"""
        fields_to_print = list(fields or DEFAULT_FIELDS) + ["filename"]
        _ = lambda x: {field: x.get(field, "(missing)") for field in fields_to_print}
        return template.format(
            ds=pprint.pformat(_(obj)), targets=pprint.pformat([_(x) for x in others])
        )

    @classmethod
    def activity_hash(
            cls, act, fields=DEFAULT_FIELDS, case_insensitive=True, strip=True
    ):
        """Hash an activity dataset.

        Used to import data formats like ecospold 1 (ecoinvent v1-2) and SimaPro, where no unique attributes for\
         datasets are given. This is clearly an imperfect and brittle solution, but there is no other obvious\
          approach at this time.

        The fields used can be optionally specified in ``fields``.

        No fields are required; an empty string is used if a field isn't present. All fields are cast to lower case.

        By default, uses the following, in order:
            * name
            * categories
            * unit
            * reference product
            * location

        Args:
            * *data* (dict): The :ref:`activity dataset data <database-documents>`.
            * *fields* (list): Optional list of fields to hash together. Default is \
            ``('name', 'categories', 'unit', 'reference product', 'location')``.
            * *case_insensitive* (bool): Cast everything to lowercase before computing hash. Default is ``True``.

        Returns:
            A MD5 hash string, hex-encoded.

        """
        string = "".join(
            [
                cls.parse_field(
                    field_value=cls.field_funcs.get(field, cls.field_funcs["default"])(
                        act, field
                    ),
                    case_insensitive=case_insensitive,
                    strip=strip,
                )
                for field in fields or DEFAULT_FIELDS
            ]
        )
        return str(hashlib.md5(string.encode("utf-8")).hexdigest())

    @classmethod
    def link_iterable_by_fields(
            cls, unlinked, other=None, fields=DEFAULT_FIELDS, kind=None, internal=False, relink=False
    ):
        """Generic function to link objects in ``unlinked`` to objects in ``other`` using fields ``fields``.

        The database to be linked must have uniqueness for each object for the given ``fields``.

        If ``kind``, limit objects in ``unlinked`` of type ``kind``.

        If ``relink``, link to objects which already have an ``input``. Otherwise, skip already linked objects.

        If ``internal``, linked ``unlinked`` to other objects in ``unlinked``. Each object must have the attributes \
        ``database`` and ``code``."""
        if internal:
            other = unlinked

        duplicates, candidates = {}, {}
        try:
            for ds in other:
                key = cls.activity_hash(ds, fields)
                if key in candidates:
                    duplicates.setdefault(key, []).append(ds)
                else:
                    candidates[key] = (ds["database"], ds["code"])
        except KeyError:
            raise StrategyError(
                "Not all datasets in database to be linked have "
                "``database`` or ``code`` attributes"
            )
        
        if isinstance(kind, str):
            kind = {kind}

        for container in unlinked:
            if relink is True:
                excs = container.get("exchanges", [])
            else:
                excs = [e for e in container.get("exchanges", []) if not e.get("input")]
            if kind:
                excs = [e for e in excs if e.get("type") in kind]
            for obj in excs:
                key = cls.activity_hash(obj, fields)
                if key in duplicates:
                    raise StrategyError(
                        cls.format_nonunique_key_error(obj, fields, duplicates[key])
                    )
                elif key in candidates:
                    obj["input"] = candidates[key]
        return unlinked

    @classmethod
    def link_activities_to_database(
            cls, activities, other=None, fields=DEFAULT_FIELDS, relink=False
    ):
        cls.link_iterable_by_fields(
            unlinked=activities, other=other or activities, fields=fields, relink=relink
        )
        return activities

    @staticmethod
    def overwrite_exchange_field_values_with_linked_activity_values(activities, fields=DEFAULT_FIELDS):
        """
        This function goes through all exchanges and copies `fields` values from the linked activity to the exchange.
        This might be helpful after linking "soft-matched" fields, such as `categories`, where a string "('air',)"
        is treated as identical to a tuple ('air',) etc.
        """
        for act in activities:
            for ex in act.get("exchanges", []):
                if "input" not in ex:
                    continue
                in_act = get_activity(ex["input"])
                for field in fields:
                    if field in in_act:
                        ex[field] = in_act[field]
        return activities

activity_hash = ExchangeLinker.activity_hash
