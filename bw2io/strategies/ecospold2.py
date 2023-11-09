import math
import warnings

from bw2data import Database
from bw2data.logs import close_log, get_io_logger
from stats_arrays import LognormalUncertainty, UndefinedUncertainty

from ..utils import es2_activity_hash, format_for_logging
from .migrations import migrate_exchanges, migrations


def link_biosphere_by_flow_uuid(db: list[dict], biosphere: str="biosphere3"):
    """
    Link the exchanges in the given list of datasets to the specified
    biosphere database by flow UUID.

    Parameters
    ----------
    db : list
        A list of datasets to be modified.
    biosphere : str, optional
        The name of the biosphere database to link to, by default "biosphere3".

    Returns
    -------
    list
        A list of the modified datasets with linked biosphere exchanges.

    Examples
    --------
    >>> from brightway2 import *
    >>> projects.set_current("my project")
    >>> db = Database("example_db")
    >>> ds1 = db.random()
    >>> ds1.new_exchange(
    ...     amount=1,
    ...     input=(("example_db", "1"),),
    ...     output=ds1.key,
    ...     type="biosphere",
    ... )
    >>> ds2 = db.random()
    >>> ds2.new_exchange(
    ...     amount=2,
    ...     input=(("biosphere3", "2"),),
    ...     output=ds2.key,
    ...     type="biosphere",
    ... )
    >>> db.write()
    >>> link_biosphere_by_flow_uuid(db)
    [{'exchanges': [{'amount': 1,
                     'input': (('example_db', '1'),),
                     'output': '63cc61954d3d9943bb32f7aa9bc33c87',
                     'type': 'production'},
                    {'amount': 1,
                     'input': (('biosphere3', '2'),),
                     'output': '63cc61954d3d9943bb32f7aa9bc33c87',
                     'type': 'biosphere'}],
      'id': '63cc61954d3d9943bb32f7aa9bc33c87',
      'type': 'process'},
     {'exchanges': [{'amount': 2,
                     'input': (('biosphere3', '2'),),
                     'output': '6f10b95c02be63e925a6f2ef6b937a6d',
                     'type': 'biosphere'}],
      'id': '6f10b95c02be63e925a6f2ef6b937a6d',
      'type': 'process'}]
    """
    biosphere_codes = {x["code"] for x in Database(biosphere)}

    for ds in db:
        for exc in ds.get("exchanges", []):
            if (
                exc.get("type") == "biosphere"
                and exc.get("flow")
                and exc.get("flow") in biosphere_codes
            ):
                exc["input"] = (biosphere, exc.get("flow"))
    return db


def remove_zero_amount_coproducts(db):
    """
    Iterate through datasets in the given database. Filter out coproducts with
    zero production amounts from the 'exchanges' list of each dataset. Return
    the updated list of datasets.
    
    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "type": "production" or "non-production",
                    "amount": float,
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with coproducts with zero production
        amounts removed from the 'exchanges' list.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {"type": "production", "amount": 0},
    ...             {"type": "production", "amount": 5},
    ...             {"type": "non-production", "amount": 0},
    ...         ]
    ...     }
    ... ]
    >>> remove_zero_amount_coproducts(db)
    [
        {
            "exchanges": [
                {"type": "production", "amount": 5},
                {"type": "non-production", "amount": 0},
            ]
        }
    ]
    """
    for ds in db:
        ds[u"exchanges"] = [
            exc
            for exc in ds["exchanges"]
            if (exc["type"] != "production" or exc["amount"])
        ]
    return db


def remove_zero_amount_inputs_with_no_activity(db):
    """
    Filter out technosphere exchanges with zero amounts and no uncertainty from
    the 'exchanges' list of each dataset in the given database. These exchanges
    are the result of the ecoinvent linking algorithm and can be safely discarded.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "uncertainty type": int,
                    "amount": float,
                    "type": "technosphere",
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with technosphere exchanges with zero
        amounts and no uncertainty removed from the 'exchanges' list.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {"uncertainty type": UndefinedUncertainty.id, "amount": 0, "type": "technosphere"},
    ...             {"uncertainty type": UndefinedUncertainty.id, "amount": 5, "type": "technosphere"},
    ...             {"uncertainty type": 2, "amount": 0, "type": "technosphere"},
    ...         ]
    ...     }
    ... ]
    >>> remove_zero_amount_inputs_with_no_activity(db)
    [
        {
            "exchanges": [
                {"uncertainty type": UndefinedUncertainty.id, "amount": 5, "type": "technosphere"},
                {"uncertainty type": 2, "amount": 0, "type": "technosphere"},
            ]
        }
    ]
    """
    for ds in db:
        ds[u"exchanges"] = [
            exc
            for exc in ds["exchanges"]
            if not (
                exc["uncertainty type"] == UndefinedUncertainty.id
                and exc["amount"] == 0
                and exc["type"] == "technosphere"
            )
        ]
    return db


def remove_unnamed_parameters(db):
    """
    Iterate through datasets in the given database and remove unnamed parameters
    from the 'parameters' dictionary of each dataset. Unnamed parameters can't be
    used in formulas or referenced.

    Parameters
    ----------
    db : list
        List of datasets, each as a dictionary containing a 'parameters' key with
        a dictionary of parameter name-value pairs. The structure of a dataset is
        as follows:

        {
            "parameters": {
                "parameter_name": {"value": parameter_value, "unnamed": boolean},
                ...
            }
        }

    Returns
    -------
    list
        Updated list of datasets with unnamed parameters removed from the
        'parameters' dictionary.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "parameters": {
    ...             "named_param": {"value": 42},
    ...             "unnamed_param": {"value": 10, "unnamed": True},
    ...         }
    ...     }
    ... ]
    >>> remove_unnamed_parameters(db)
    [
        {
            "parameters": {
                "named_param": {"value": 42},
            }
        }
    ]
    """
    for ds in db:
        if "parameters" in ds:
            ds["parameters"] = {
                key: value
                for key, value in ds["parameters"].items()
                if not value.get("unnamed")
            }
    return db


def es2_assign_only_product_with_amount_as_reference_product(db):
    """
    If a multioutput process has one product with a non-zero amount, this
    function assigns that product as the reference product. This is typically
    called after `remove_zero_amount_coproducts`, which will delete the
    zero-amount coproducts. However, the zero-amount logic is still kept in
    case users want to keep all coproducts.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "type": "production",
                    "amount": float,
                    "name": str,
                    "flow": str,
                    "unit": str,
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with the non-zero amount product assigned
        as the reference product for multioutput processes.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {"type": "production", "amount": 0, "name": "A", "flow": "flow_A", "unit": "kg"},
    ...             {"type": "production", "amount": 5, "name": "B", "flow": "flow_B", "unit": "kg"},
    ...         ]
    ...     }
    ... ]
    >>> es2_assign_only_product_with_amount_as_reference_product(db)
    [
        {
            "exchanges": [
                {"type": "production", "amount": 0, "name": "A", "flow": "flow_A", "unit": "kg"},
                {"type": "production", "amount": 5, "name": "B", "flow": "flow_B", "unit": "kg"},
            ],
            "reference product": "B",
            "flow": "flow_B",
            "unit": "kg",
            "production amount": 5,
        }
    ]
    """
    for ds in db:
        amounted = [
            prod
            for prod in ds["exchanges"]
            if prod["type"] == "production" and prod["amount"]
        ]
        # OK if it overwrites existing reference product; need flow as well
        if len(amounted) == 1:
            ds[u"reference product"] = amounted[0]["name"]
            ds[u"flow"] = amounted[0][u"flow"]
            if not ds.get("unit"):
                ds[u"unit"] = amounted[0]["unit"]
            ds[u"production amount"] = amounted[0]["amount"]
    return db


def assign_single_product_as_activity(db):
    """
    Assign the activity of a dataset to the 'activity' field of the production
    exchange for datasets with only one production exchange.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries and an 'activity'
        key with the activity name. The dataset dictionary has the following
        structure:
        dataset: dict = {
            "activity": "activity_A",
            "exchanges": [
                {"type": "production", "name": "product_A"},
                {"type": "non-production", "name": "input_A"},
            ],
        }

    Returns
    -------
    list
        The updated list of datasets with the activity assigned to the single
        production exchange.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "activity": "activity_A",
    ...         "exchanges": [
    ...             {"type": "production", "name": "product_A"},
    ...             {"type": "non-production", "name": "input_A"},
    ...         ],
    ...     }
    ... ]
    >>> assign_single_product_as_activity(db)
    [
        {
            "activity": "activity_A",
            "exchanges": [
                {"type": "production", "name": "product_A", "activity": "activity_A"},
                {"type": "non-production", "name": "input_A"},
            ],
        }
    ]
    """
    for ds in db:
        prod_exchanges = [
            exc for exc in ds.get("exchanges") if exc["type"] == "production"
        ]
        # raise ValueError
        if len(prod_exchanges) == 1:
            prod_exchanges[0]["activity"] = ds["activity"]
    return db


def create_composite_code(db):
    """
    Generate a composite code for each dataset in the given database using the
    activity and flow names. Assign the composite code to the 'code' field of
    the dataset.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing
        'activity' and 'flow' keys with their respective names. The dataset
        dictionary has the following structure:
        dataset: dict = {
            "activity": "activity_A",
            "flow": "flow_A",
        }

    Returns
    -------
    list
        The updated list of datasets with the composite code assigned to the
        'code' field.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "activity": "activity_A",
    ...         "flow": "flow_A",
    ...     }
    ... ]
    >>> create_composite_code(db)
    [
        {
            "activity": "activity_A",
            "flow": "flow_A",
            "code": es2_activity_hash("activity_A", "flow_A"),
        }
    ]
    """
    for ds in db:
        ds[u"code"] = es2_activity_hash(ds["activity"], ds["flow"])
    return db


def link_internal_technosphere_by_composite_code(db):
    """
    Link internal technosphere inputs in the database by their composite code.
    Only link to process datasets that are present in the database document.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing a
        'code' key, a 'database' key, and an 'exchanges' key with a list of
        exchange dictionaries. The dataset dictionary has a nested structure
        for the 'exchanges' key, as follows:
        dataset: dict = {
            "database": "db_A",
            "code": es2_activity_hash("activity_A", "flow_A"),
            "exchanges": [
                {
                    "type": "technosphere",
                    "activity": "activity_A",
                    "flow": "flow_A",
                },
                ...
            ],
        }

    Returns
    -------
    list
        The updated list of datasets with internal technosphere inputs linked
        by composite code.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "database": "db_A",
    ...         "code": es2_activity_hash("activity_A", "flow_A"),
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "activity": "activity_A",
    ...                 "flow": "flow_A",
    ...             }
    ...         ],
    ...     }
    ... ]
    >>> link_internal_technosphere_by_composite_code(db)
    [
        {
            "database": "db_A",
            "code": es2_activity_hash("activity_A", "flow_A"),
            "exchanges": [
                {
                    "type": "technosphere",
                    "activity": "activity_A",
                    "flow": "flow_A",
                    "input": ("db_A", es2_activity_hash("activity_A", "flow_A")),
                }
            ],
        }
    ]
    """
    candidates = {ds["code"] for ds in db}
    for ds in db:
        for exc in ds.get("exchanges", []):
            if (
                exc["type"]
                in {
                    "technosphere",
                    "production",
                    "substitution",
                }
                and exc.get("activity")
            ):
                key = es2_activity_hash(exc["activity"], exc["flow"])
                if key in candidates:
                    exc[u"input"] = (ds["database"], key)
    return db


def delete_exchanges_missing_activity(db):
    """
    Remove exchanges that are missing the "activityLinkId" attribute and have
    flows that are not produced as the reference product of any activity. See
    the `known data issues <http://www.ecoinvent.org/database/ecoinvent-version-3/reports-of-changes/known-data-issues/>`__ report.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The dataset
        dictionary has a nested structure for the 'exchanges' key, as follows:
        dataset: dict = {
            "filename": "file_A",
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "unlinked_exchange",
                },
                ...
            ],
        }

    Returns
    -------
    list
        The updated list of datasets with unlinked exchanges removed.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "filename": "file_A",
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "unlinked_exchange",
    ...             },
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "linked_exchange",
    ...                 "input": ("db_A", "code_A"),
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> delete_exchanges_missing_activity(db)
    [
        {
            "filename": "file_A",
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "linked_exchange",
                    "input": ("db_A", "code_A"),
                },
            ],
        }
    ]
    """
    log, logfile = get_io_logger("Ecospold2-import-error")
    count = 0
    for ds in db:
        exchanges = ds.get("exchanges", [])
        if not exchanges:
            continue
        skip = []
        for exc in exchanges:
            if exc.get("input"):
                continue
            if not exc.get("activity") and exc["type"] in {
                "technosphere",
                "production",
                "substitution",
            }:
                log.critical(
                    u"Purging unlinked exchange:\nFilename: {}\n{}".format(
                        ds[u"filename"], format_for_logging(exc)
                    )
                )
                count += 1
                skip.append(exc)
        ds[u"exchanges"] = [exc for exc in exchanges if exc not in skip]
    close_log(log)
    if count:
        print(
            (
                u"{} exchanges couldn't be linked and were deleted. See the "
                u"logfile for details:\n\t{}"
            ).format(count, logfile)
        )
    return db


def delete_ghost_exchanges(db):
    """
    Remove ghost exchanges from the given database. A ghost exchange is one
    that links to a combination of activity and flow which aren't provided
    in the database.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The dataset
        dictionary has a nested structure for the 'exchanges' key, as follows:
        dataset: dict = {
            "filename": "file_A",
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "ghost_exchange",
                },
                ...
            ],
        }

    Returns
    -------
    list
        The updated list of datasets with ghost exchanges removed.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "filename": "file_A",
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "ghost_exchange",
    ...             },
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "linked_exchange",
    ...                 "input": ("db_A", "code_A"),
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> delete_ghost_exchanges(db)
    [
        {
            "filename": "file_A",
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "linked_exchange",
                    "input": ("db_A", "code_A"),
                },
            ],
        }
    ]
    """
    log, logfile = get_io_logger("Ecospold2-import-error")
    count = 0
    for ds in db:
        exchanges = ds.get("exchanges", [])
        if not exchanges:
            continue
        skip = []
        for exc in exchanges:
            if exc.get("input") or exc.get("type") != "technosphere":
                continue
            log.critical(
                u"Purging unlinked exchange:\nFilename: {}\n{}".format(
                    ds[u"filename"], format_for_logging(exc)
                )
            )
            count += 1
            skip.append(exc)
        ds[u"exchanges"] = [exc for exc in exchanges if exc not in skip]
    close_log(log)
    if count:
        print(
            (
                u"{} exchanges couldn't be linked and were deleted. See the "
                u"logfile for details:\n\t{}"
            ).format(count, logfile)
        )
    return db


def remove_uncertainty_from_negative_loss_exchanges(db):
    """
    Address cases where basic uncertainty and pedigree matrix are applied blindly,
    producing strange net production values. Assume these loss factors are static
    and only apply to exchanges that decrease net production.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "type": str,
                    "name": str,
                    "amount": float,
                    "uncertainty type": int,
                    "loc": float,
                    "scale": float,
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with uncertainty removed from negative
        lognormal exchanges.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "production",
    ...                 "name": "product_A",
    ...                 "amount": 10,
    ...             },
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "product_A",
    ...                 "amount": -2,
    ...                 "uncertainty type": 2,
    ...                 "loc": -2,
    ...                 "scale": 0.1,
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> remove_uncertainty_from_negative_loss_exchanges(db)
    [
        {
            "exchanges": [
                {
                    "type": "production",
                    "name": "product_A",
                    "amount": 10,
                },
                {
                    "type": "technosphere",
                    "name": "product_A",
                    "amount": -2,
                    "uncertainty type": 0,
                    "loc": -2,
                },
            ],
        }
    ]
    Notes
    --------
    There are 15699 of these in ecoinvent 3.3 cutoff.
    """
    for ds in db:
        production_names = {
            exc["name"]
            for exc in ds.get("exchanges", [])
            if exc["type"] == "production"
        }
        for exc in ds.get("exchanges", []):
            if (
                exc["amount"] < 0
                and exc["uncertainty type"] == LognormalUncertainty.id
                and exc["name"] in production_names
            ):
                exc["uncertainty type"] = UndefinedUncertainty.id
                exc["loc"] = exc["amount"]
                del exc["scale"]
    return db


def set_lognormal_loc_value(db):
    """
    Ensure loc value is correct for lognormal uncertainty distributions.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "type": str,
                    "name": str,
                    "amount": float,
                    "uncertainty type": int,
                    "loc": float,
                    "scale": float,
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with correct lognormal uncertainty
        distribution loc values.

    Examples
    --------
    >>> import math
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "input_A",
    ...                 "amount": 5,
    ...                 "uncertainty type": 2,
    ...                 "loc": 1,
    ...                 "scale": 0.5,
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> set_lognormal_loc_value(db)
    [
        {
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "input_A",
                    "amount": 5,
                    "uncertainty type": 2,
                    "loc": math.log(5),
                    "scale": 0.5,
                },
            ],
        }
    ]
    """
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc["uncertainty type"] == LognormalUncertainty.id:
                exc["loc"] = math.log(abs(exc["amount"]))
    return db


def reparametrize_lognormal_to_agree_with_static_amount(db):
    """
    For lognormal distributions, choose the mean of the underlying normal distribution
    (loc) such that the expected value (mean) of the resulting distribution is
    equal to the (static) amount defined for the exchange.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "type": str,
                    "name": str,
                    "amount": float,
                    "uncertainty type": int,
                    "loc": float,
                    "scale": float,
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with adjusted lognormal uncertainty
        distribution loc values.

    Examples
    --------
    >>> import math
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "input_A",
    ...                 "amount": 5,
    ...                 "uncertainty type": 2,
    ...                 "loc": 1,
    ...                 "scale": 0.5,
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> reparametrize_lognormals_to_agree_with_static_amount(db)
    [
        {
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "input_A",
                    "amount": 5,
                    "uncertainty type": 2,
                    "loc": math.log(5) - 0.5**2 / 2,
                    "scale": 0.5,
                },
            ],
        }
    ]
    """
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc["uncertainty type"] == LognormalUncertainty.id:
                exc["loc"] = math.log(abs(exc["amount"])) - exc["scale"]**2 / 2
    return db


def fix_unreasonably_high_lognormal_uncertainties(db, cutoff=2.5, replacement=0.25):
    """
    Replace unreasonably high lognormal uncertainties in the given database
    with a specified replacement value. With the default cutoff value of 2.5
    and a median of 1, the 95% confidence interval has a high to low ratio of 20.000.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The dataset
        dictionary has a nested structure for the 'exchanges' key, as follows:
        dataset: dict = {
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "input_A",
                    "amount": 5,
                    "uncertainty type": 2,
                    "loc": 5,
                    "scale": 3,
                },
                ...
            ],
        }
    cutoff : float, optional
        The cutoff value above which an uncertainty value is considered
        unreasonably high (default is 2.5).
    replacement : float, optional
        The replacement value for unreasonably high uncertainties (default is 0.25).

    Returns
    -------
    list
        The updated list of datasets with unreasonably high uncertainties fixed.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "input_A",
    ...                 "amount": 5,
    ...                 "uncertainty type": 2,
    ...                 "loc": 5,
    ...                 "scale": 3,
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> fix_unreasonably_high_lognormal_uncertainties(db)
    [
        {
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "input_A",
                    "amount": 5,
                    "uncertainty type": 2,
                    "loc": 5,
                    "scale": 0.25,
                },
            ],
        }
    ]
    """
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc["uncertainty type"] == LognormalUncertainty.id:
                if exc["scale"] > cutoff:
                    exc["scale"] = replacement
    return db


def fix_ecoinvent_flows_pre35(db):
    """
    Apply the 'fix-ecoinvent-flows-pre-35' migration to the given database if 
    available; otherwise, raise a warning and return the unmodified database.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The dataset 
        dictionary has a nested structure for the 'exchanges' key, as follows:
        dataset: dict = {
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "input_A",
                    "amount": 5,
                },
                ...
            ],
        }

    Returns
    -------
    list
        The updated list of datasets with ecoinvent flows fixed, or the
        original list of datasets if the migration is not available.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "technosphere",
    ...                 "name": "input_A",
    ...                 "amount": 5,
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> fix_ecoinvent_flows_pre35(db)
    [
        {
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "input_A",
                    "amount": 5,
                },
            ],
        }
    ]
    """
    if "fix-ecoinvent-flows-pre-35" in migrations:
        return migrate_exchanges(db, "fix-ecoinvent-flows-pre-35")
    else:
        warnings.warn(
            (
                "Skipping migration 'fix-ecoinvent-flows-pre-35' "
                "because it isn't installed"
            )
        )
        return db


def drop_temporary_outdated_biosphere_flows(db):
    """
    Removes exchanges with specific temporary biosphere flow names from the
    given database. Drop biosphere exchanges which aren't used and are outdated.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries. The structure of a
        dataset is as follows:

        {
            "exchanges": [
                {
                    "type": str,
                    "name": str,
                    "amount": float,
                },
                ...
            ]
        }

    Returns
    -------
    list
        The updated list of datasets with outdated temporary biosphere exchanges removed.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "biosphere",
    ...                 "name": "Fluorene_temp",
    ...                 "amount": 5,
    ...             },
    ...             {
    ...                 "type": "biosphere",
    ...                 "name": "valid_biosphere_flow",
    ...                 "amount": 10,
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> drop_temporary_outdated_biosphere_flows(db)
    [
        {
            "exchanges": [
                {
                    "type": "biosphere",
                    "name": "valid_biosphere_flow",
                    "amount": 10,
                },
            ],
        }
    ]
    """
    names = {
        "Fluorene_temp",
        "Fluoranthene_temp",
        "Dibenz(a,h)anthracene_temp",
        "Benzo(k)fluoranthene_temp",
        "Benzo(ghi)perylene_temp",
        "Benzo(b)fluoranthene_temp",
        "Benzo(a)anthracene_temp",
        "Acenaphthylene_temp",
        "Chrysene_temp",
        "Pyrene_temp",
        "Phenanthrene_temp",
        "Indeno(1,2,3-c,d)pyrene_temp",
    }
    for ds in db:
        ds["exchanges"] = [
            obj
            for obj in ds["exchanges"]
            if not (obj.get("name") in names and obj.get("type") == "biosphere")
        ]
    return db


def add_cpc_classification_from_single_reference_product(db):
    """
    Add CPC classification to a dataset's classifications if it has only one 
    reference product with a CPC classification.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an
        'exchanges' key with a list of exchange dictionaries and a
        'classifications' key with a list of classification tuples. The dataset 
        dictionary has a nested structure for the 'exchanges' key, as follows:
        dataset: dict = {
            "exchanges": [
                {
                    "type": "production",
                    "classifications": {"CPC": ["code"]},
                },
                ...
            ],
            "classifications": [],
        }

    Returns
    -------
    list
        The updated list of datasets with CPC classification added to datasets
        from their single reference product.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "classifications": [],
    ...         "exchanges": [
    ...             {
    ...                 "type": "production",
    ...                 "classifications": {"CPC": ["code"]},
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> add_cpc_classification_from_single_reference_product(db)
    [
        {
            "classifications": [("CPC", "code")],
            "exchanges": [
                {
                    "type": "production",
                    "classifications": {"CPC": ["code"]},
                },
            ],
        }
    ]
    """
    def has_cpc(exc):
        return (
            "classifications" in exc
            and "CPC" in exc["classifications"]
            and exc["classifications"]["CPC"]
        )

    for ds in db:
        assert "classifications" in ds
        products = [exc for exc in ds["exchanges"] if exc["type"] == "production"]
        if len(products) == 1 and has_cpc(products[0]):
            ds["classifications"].append(
                ("CPC", products[0]["classifications"]["CPC"][0])
            )
    return db


def delete_none_synonyms(db):
    """
    Remove `None` values from the 'synonyms' list of each dataset.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing a
        'synonyms' key with a list of synonyms. The dataset dictionary has a nested
        structure for the 'parameters' key, as follows:
        dataset: dict = {
            "parameters": {
                "parameter1": {"synonyms": ["synonym1", None, "synonym2"]},
                "parameter2": {"synonyms": ["synonym3", "synonym4"]},
                ...
            }
        }

    Returns
    -------
    list
        The updated list of datasets with None values removed from the
        'synonyms' list.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "parameters": {
    ...             "parameter1": {"synonyms": ["synonym1", None, "synonym2"]},
    ...             "parameter2": {"synonyms": ["synonym3", "synonym4"]},
    ...         }
    ...     },
    ... ]
    >>> delete_none_synonyms(db)
    [
        {
            "parameters": {
                "parameter1": {"synonyms": ["synonym1", "synonym2"]},
                "parameter2": {"synonyms": ["synonym3", "synonym4"]},
            }
        },
    ]
    """
    for ds in db:
        ds["synonyms"] = [s for s in ds["synonyms"] if s is not None]
    return db


def update_social_flows_in_older_consequential(db, biosphere_db):
    """
    Update the UUIDs of specific biosphere flows with the category 'social' in older consequential datasets. 
    These flows are not used, and their UUIDs change with each release. The ecoinvent centre recommends dropping them,
    but this function replaces their UUIDs instead.

    Parameters
    ----------
    db : list
        A list of datasets, where each dataset is a dictionary containing an 'exchanges' key with a list
        of exchange dictionaries. These datasets represent the main data to be updated.
    biosphere_db : list
        A list of biosphere datasets, where each dataset is a dictionary containing flow information.
        These datasets provide the updated UUIDs for the specific social flows.

    Returns
    -------
    list
        The updated list of datasets with the UUIDs of the specified social flows replaced.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "name": "residual wood, dry",
    ...                 "input": "old_uuid",
    ...             },
    ...         ],
    ...     },
    ... ]
    >>> biosphere_db = [
    ...     {
    ...         "name": "residual wood, dry",
    ...         "key": "new_uuid",
    ...     },
    ... ]
    >>> update_social_flows_in_older_consequential(db, biosphere_db)
    [
        {
            "exchanges": [
                {
                    "name": "residual wood, dry",
                    "input": "new_uuid",
                },
            ],
        },
    ]
    """
    FLOWS = {
        'residual wood, dry',
        'venting of argon, crude, liquid',
        'venting of nitrogen, liquid',
    }

    cache = {}

    def get_cache(cache, biosphere_db):
        for flow in biosphere_db:
            if flow['name'] in FLOWS:
                cache[flow['name']] = flow.key

    for ds in db:
        for exc in ds['exchanges']:
            if not exc.get('input') and exc['name'] in FLOWS:
                if not cache:
                    get_cache(cache, biosphere_db)
                exc['input'] = cache[exc['name']]
    return db
