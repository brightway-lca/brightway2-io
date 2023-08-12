import collections
import copy

from bw2data import Database

from ..utils import activity_hash


def add_activity_hash_code(data):
    """
    Add 'code' field to characterization factors using 'activity_hash', if 'code' not already present.

    Iterates over the LCIA methods in the given data and adds a 'code' field to each characterization
    factor using the 'activity_hash' function, if the 'code' field is not already present.

    Parameters
    ----------
    data : list
        A list of dictionaries representing LCIA methods with characterization factors.

    Returns
    -------
    list
        A list of dictionaries representing the updated LCIA methods with 'code' field added to characterization factors.

    Examples
    --------
    >>> data = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "name": "Characterization Factor 1",
    ...                 # Other fields needed for activity_hash function
    ...             },
    ...             {
    ...                 "name": "Characterization Factor 2",
    ...                 "code": "existing_code",
    ...                 # Other fields needed for activity_hash function
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> add_activity_hash_code(data)
    [
        {
            'exchanges': [
                {
                    'name': 'Characterization Factor 1',
                    'code': 'generated_code',
                    # Other fields needed for activity_hash function
                },
                {
                    'name': 'Characterization Factor 2',
                    'code': 'existing_code',
                    # Other fields needed for activity_hash function
                },
            ],
        }
    ]
    """
    for method in data:
        for cf in method["exchanges"]:
            if cf.get("code"):
                continue
            cf[u"code"] = activity_hash(cf)
    return data


def drop_unlinked_cfs(data):
    """
    Drop characterization factors (CFs) that don't have an 'input' attribute.

    Iterates over the LCIA methods in the given data and removes any characterization factors that
    don't have an 'input' attribute.

    Parameters
    ----------
    data : list
        A list of dictionaries representing LCIA methods with characterization factors.

    Returns
    -------
    list
        A list of dictionaries representing the updated LCIA methods with unlinked characterization factors removed.

    Examples
    --------
    >>> data = [
    ...     {
    ...         "exchanges": [
    ...             {"name": "Characterization Factor 1", "input": "input_1"},
    ...             {"name": "Characterization Factor 2"},
    ...         ],
    ...     }
    ... ]
    >>> drop_unlinked_cfs(data)
    [
        {
            'exchanges': [
                {
                    'name': 'Characterization Factor 1',
                    'input': 'input_1',
                },
            ],
        }
    ]
    """
    for method in data:
        method[u"exchanges"] = [
            cf for cf in method["exchanges"] if cf.get("input") is not None
        ]
    return data


def set_biosphere_type(data):
    """
    Set characterization factor (CF) types to 'biosphere' for compatibility with LCI strategies.

    Iterates over the LCIA methods in the given data and sets the 'type' attribute of each
    characterization factor to 'biosphere'. This will overwrite existing 'type' values.

    Parameters
    ----------
    data : list
        A list of dictionaries representing LCIA methods with characterization factors.

    Returns
    -------
    list
        A list of dictionaries representing the updated LCIA methods with 'biosphere' set as the 'type' of
        characterization factors.

    Examples
    --------
    >>> data = [
    ...     {
    ...         "exchanges": [
    ...             {"name": "Characterization Factor 1", "type": "original_type"},
    ...             {"name": "Characterization Factor 2"},
    ...         ],
    ...     }
    ... ]
    >>> set_biosphere_type(data)
    [
        {
            'exchanges': [
                {
                    'name': 'Characterization Factor 1',
                    'type': 'biosphere',
                },
                {
                    'name': 'Characterization Factor 2',
                    'type': 'biosphere',
                },
            ],
        }
    ]
    """
    for method in data:
        for cf in method["exchanges"]:
            cf[u"type"] = u"biosphere"
    return data


def rationalize_method_names(data):
    """
    Rationalize LCIA method names by removing redundant parts and unifying naming conventions.

    Iterates over the LCIA methods in the given data and updates the 'name' attribute of each method
    to remove unnecessary information and make the naming conventions more consistent.

    Parameters
    ----------
    data : list
        A list of dictionaries representing LCIA methods with method names.

    Returns
    -------
    list
        A list of dictionaries representing the updated LCIA methods with rationalized method names.

    Examples
    --------
    >>> data = [
    ...     {"name": ("Method 1 w/o LT", "Total")},
    ...     {"name": ("Method 2 no LT", "Total")},
    ...     {"name": ("Method 3", "Total")},
    ... ]
    >>> rationalize_method_names(data)
    [
        {'name': ('Method 1', 'without long-term')},
        {'name': ('Method 2', 'without long-term')},
        {'name': ('Method 3',)},
    ]
    """
    counts = collections.Counter()
    for obj in data:
        if isinstance(obj["name"], tuple):
            counts[obj["name"][:2]] += 1

    for obj in data:
        if not isinstance(obj["name"], tuple):
            continue

        if " w/o LT" in obj["name"][0]:
            obj["name"] = tuple([o.replace(" w/o LT", "") for o in obj["name"]]) + (
                "without long-term",
            )
        if " no LT" in obj["name"][0]:
            obj["name"] = tuple([o.replace(" no LT", "") for o in obj["name"]]) + (
                "without long-term",
            )
        elif {o.lower() for o in obj["name"][1:]} == {"total"}:
            obj["name"] = obj["name"][:2]
        elif len(obj["name"]) > 2 and obj["name"][1].lower() == "total":
            obj["name"] = (obj["name"][0],) + obj["name"][2:]
        elif obj["name"][-1].lower() == "total" and counts[obj["name"][:2]] == 1:
            obj["name"] = obj["name"][:2]

    return data


def match_subcategories(data, biosphere_db_name, remove=True):
    """
    Add CFs for biosphere flows with the same top-level categories as a given characterization.

    Given a characterization with a top-level category, e.g. ('air',), Finds all biosphere flows with
    the same top-level categories and adds CFs for these flows as well. It doesn't replace CFs for existing flows
    with multi-level categories. If `remove` is set to True, it also deletes the top-level CF, but only if it is
    unlinked.

    Parameters
    ----------
    data : list
        A list of dictionaries representing LCIA methods with characterization factors.
    biosphere_db_name : str
        The name of the biosphere database to look up flows.
    remove : bool, optional
        If True, delete the top-level CF if it is unlinked. Default is True.

    Returns
    -------
    list
        A list of dictionaries representing the updated LCIA methods with CFs added for biosphere flows with the
        same top-level categories.

    Examples
    --------
    >>> data = [
    ...     {
    ...         "name": "Method 1",
    ...         "exchanges": [
    ...             {"categories": ("air",), "name": "Emission", "unit": "kg", "amount": 1.0},
    ...         ],
    ...     }
    ... ]
    >>> biosphere_db_name = "example_biosphere"
    >>> match_subcategories(data, biosphere_db_name)
    [
        {
            'name': 'Method 1',
            'exchanges': [
                {'categories': ('air',), 'name': 'Emission', 'unit': 'kg', 'amount': 1.0},
                # Additional CFs for biosphere flows with the same top-level category ('air',)
            ],
        }
    ]
    """
    def add_amount(obj, amount):
        obj["amount"] = amount
        return obj

    def add_subcategories(obj, mapping):
        # Sorting needed for tests
        new_objs = sorted(
            mapping[
                (
                    obj["categories"][0],
                    obj["name"],
                    obj["unit"],
                )
            ],
            key=lambda x: tuple([x[key] for key in sorted(x.keys())]),
        )
        # Need to create copies so data from later methods doesn't
        # clobber amount values
        return [add_amount(copy.deepcopy(elem), obj["amount"]) for elem in new_objs]

    mapping = collections.defaultdict(list)
    for flow in Database(biosphere_db_name):
        # Try to filter our industrial activities and their flows
        if not flow.get("type") in ("emission", "natural resource"):
            continue
        if len(flow.get("categories", [])) > 1:
            mapping[(flow["categories"][0], flow["name"], flow["unit"])].append(
                {
                    "categories": flow["categories"],
                    "database": flow["database"],
                    "input": flow.key,
                    "name": flow["name"],
                    "unit": flow["unit"],
                }
            )

    for method in data:
        already_have = {(obj["name"], obj["categories"]) for obj in method["exchanges"]}

        new_cfs = []
        for obj in method["exchanges"]:
            if len(obj["categories"]) > 1:
                continue
            # Don't add subcategory flows which already have CFs
            subcat_cfs = [
                x
                for x in add_subcategories(obj, mapping)
                if (x["name"], x["categories"]) not in already_have
            ]
            if subcat_cfs and remove and not obj.get("input"):
                obj["remove_me"] = True
            new_cfs.extend(subcat_cfs)
        method[u"exchanges"].extend(new_cfs)
        if remove:
            method[u"exchanges"] = [
                obj for obj in method["exchanges"] if not obj.get("remove_me")
            ]
    return data


def fix_ecoinvent_38_lcia_implementation(data):
    """
    Update flow names in ecoinvent 3.8 LCIA implementation to correct inconsistencies.

    Ecoinvent 3.8 LCIA implementation uses some flow names from 3.7. Updates these flow names when
    possible and deletes them when not.

    Parameters
    ----------
    data : list
        A list of dictionaries representing LCIA methods with characterization factors.

    Returns
    -------
    list
        A list of dictionaries representing the updated LCIA methods with corrected flow names.

    Examples
    --------
    >>> data = [
    ...     {
    ...         "name": "Method 1",
    ...         "exchanges": [
    ...             {"name": "Cyfluthrin", "categories": ("soil", "agricultural")},
    ...         ],
    ...     }
    ... ]
    >>> fix_ecoinvent_38_lcia_implementation(data)
    [
        {
            "name": "Method 1",
            "exchanges": [
                {"name": "Beta-cyfluthrin", "categories": ("soil", "agricultural")},
            ],
        }
    ]

    Notes
    -----
    The function includes a hardcoded mapping to fix known inconsistencies in flow names. This may not cover all
    possible inconsistencies and might need to be updated in the future.
    """
    MAPPING = {
        (
            "Cyfluthrin",
            ("soil", "agricultural"),
        ): "Beta-cyfluthrin",  # Note: Not the same thing!!!
        (
            "Cyfluthrin",
            ("air", "non-urban air or from high stacks"),
        ): "Beta-cyfluthrin",  # Note: Not the same thing!!!
        (
            "Carfentrazone ethyl ester",
            ("soil", "agricultural"),
        ): "[Deleted]Carfentrazone ethyl ester",  # Note: Seriously, WTF!?
        (
            "Tri-allate",
            ("soil", "agricultural"),
        ): "Triallate",  # But now there is ALSO a flow called "[Deleted]Tri-allate" into agricultural soil!
        (
            "Thiophanat-methyl",
            ("soil", "agricultural"),
        ): "Thiophanate-methyl",  # Why not? Keep them on their toes! But please make sure to change it back in 3.9.
    }
    REMOVE = {
        ("Flurochloridone", ("soil", "agricultural")),
        ("Chlorotoluron", ("soil", "agricultural")),
    }
    for method in data:
        method["exchanges"] = [
            cf
            for cf in method["exchanges"]
            if (cf["name"], cf["categories"]) not in REMOVE
        ]
        for cf in method["exchanges"]:
            try:
                cf["name"] = MAPPING[(cf["name"], cf["categories"])]
            except KeyError:
                pass
    return data
