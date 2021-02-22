# -*- coding: utf-8 -*-
import copy


def delete_integer_codes(data):
    """Delete integer codes completely from extracted ecospold1 datasets"""
    for ds in data:
        if "code" in ds and isinstance(ds["code"], int):
            del ds["code"]
        for exc in ds.get("exchanges", []):
            if "code" in exc and isinstance(exc["code"], int):
                del exc["code"]
    return data


def clean_integer_codes(data):
    """Convert integer activity codes to strings and delete integer codes from exchanges (they can't be believed)."""
    for ds in data:
        if "code" in ds and isinstance(ds["code"], int):
            ds["code"] = str(ds["code"])
        for exc in ds.get("exchanges", []):
            if "code" in exc and isinstance(exc["code"], int):
                del exc["code"]
    return data


def es1_allocate_multioutput(data):
    """This strategy allocates multioutput datasets to new datasets.

    This deletes the multioutput dataset, breaking any existing linking. This shouldn't be a concern, as you shouldn't link to a multioutput dataset in any case.

    Note that multiple allocations for the same product and input will result in undefined behavior.

    """
    activities = []
    for ds in data:
        if ds.get("allocations"):
            for activity in allocate_exchanges(ds):
                del activity["allocations"]
                activities.append(activity)
        else:
            activities.append(ds)
    return activities


def allocate_exchanges(ds):
    """
Take a dataset, which has multiple outputs, and return a list of allocated datasets.

The allocation data structure looks like:

.. code-block:: python

    {
        'exchanges': [integer codes for biosphere flows, ...],
        'fraction': out of 100,
        'reference': integer codes
    }

We assume that the allocation factor for each coproduct is always 100 percent.

    """
    new_datasets = []
    coproducts = [exc for exc in ds["exchanges"] if exc["type"] == "production"]
    multipliers = {}
    for obj in ds["allocations"]:
        if not obj["fraction"]:
            continue
        for exc_id in obj["exchanges"]:
            multipliers.setdefault(obj["reference"], {})[exc_id] = obj["fraction"] / 100
    exchange_dict = {
        exc["code"]: exc for exc in ds["exchanges"] if exc["type"] != "production"
    }
    for coproduct in coproducts:
        new_ds = copy.deepcopy(ds)
        new_ds["exchanges"] = [
            rescale_exchange(exchange_dict[exc_id], scale)
            for exc_id, scale in list(multipliers[coproduct["code"]].items())
            # Exclude self-allocation; assume 100%
            if exc_id != coproduct["code"]
        ] + [coproduct]
        new_datasets.append(new_ds)
    return new_datasets


def rescale_exchange(exc, scale):
    exc = copy.deepcopy(exc)
    exc["amount"] *= scale
    return exc
