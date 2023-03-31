from .migrations import migrate_datasets, migrate_exchanges


def drop_unspecified_subcategories(db):
    """Drop subcategories if they are in the following:
    * ``unspecified``
    * ``(unspecified)``
    * ``''`` (empty string)
    * ``None``

    Parameters
    ----------
    db : list
        A list of datasets, each containing exchanges.
    
    Returns
    ----------
    list
        A modified list of datasets with unspecified subcategories removed.
    
    Examples
    ----------
    >>> db = [{"categories": ["A", "unspecified"]},
                {"exchanges": [{"categories": ["B", ""]}]},
                {"categories": ["C", None]}]
    >>> new_db = drop_unspecified_subcategories(db)
    >>> new_db
    [{"categories": ["A"]}, {"exchanges": [{"categories": ["B"]}]}, {"categories": ["C"]}]
    """
    UNSPECIFIED = {"unspecified", "(unspecified)", "", None}
    for ds in db:
        if ds.get("categories"):
            while ds["categories"] and ds["categories"][-1] in UNSPECIFIED:
                ds["categories"] = ds["categories"][:-1]
        for exc in ds.get("exchanges", []):
            if exc.get("categories"):
                while exc["categories"] and exc["categories"][-1] in UNSPECIFIED:
                    exc["categories"] = exc["categories"][:-1]
    return db


def normalize_biosphere_names(db, lcia=False):
    """Normalize biosphere flow names to ecoinvent 3.1 standard in the given database.

    Assumes that each dataset and each exchange have a ``name``. Will change names even if exchange is already linked.
    
    Parameters
    ----------
    db : list
        A list of datasets, each containing exchanges.
    lcia : bool, optional
        If True, only normalize biosphere flow names in LCIA datasets. Default is False.
        
    Returns
    ----------
    list
        A modified list of datasets with normalized biosphere flow names.
        
    Examples
    ----------
    >>> db = [{"name": "old_biosphere_name"}]
    >>> new_db = normalize_biosphere_names(db)
    >>> new_db
    [{"name": "new_biosphere_name"}]
    """
    db = migrate_exchanges(db, migration="biosphere-2-3-names")
    if not lcia:
        db = migrate_datasets(db, migration="biosphere-2-3-names")
    return db


def normalize_biosphere_categories(db, lcia=False):
    """
    Normalize biosphere categories to ecoinvent 3.1 standard in the given database.

    Parameters
    ----------
    db : list
        A list of datasets, each containing exchanges.
    lcia : bool, optional
        If True, only normalize biosphere categories in LCIA datasets. Defaults to False.

    Returns
    -------
    list
        A modified list of datasets with normalized biosphere categories.

    Examples
    --------
    >>> db = [{"categories": ["old_biosphere_category"]}]
    >>> new_db = normalize_biosphere_categories(db)
    >>> new_db
    [{"categories": ["new_biosphere_category"]}]
    """
    db = migrate_exchanges(db, migration="biosphere-2-3-categories")
    if not lcia:
        db = migrate_datasets(db, migration="biosphere-2-3-categories")
    return db


def strip_biosphere_exc_locations(db):
    """
    Remove locations from biosphere exchanges in the given database, as biosphere exchanges are not geographically specific.
    
    Parameters
    -------
    db : list
        A list of datasets, each containing exchanges.
        
    Returns
    -------
    list
        A modified list of datasets with locations removed from biosphere exchanges.
    
    Examples
    --------
    >>> db = [{"exchanges": [{"type": "biosphere", "location": "GLO"}]}]
    >>> new_db = strip_biosphere_exc_locations(db)
    >>> new_db
    [{"exchanges": [{"type": "biosphere"}]}]
    """
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc.get("type") == "biosphere" and "location" in exc:
                del exc["location"]
    return db


def ensure_categories_are_tuples(db):
    """
    Convert dataset categories to tuples in the given database, if they are not already tuples.

    Parameters
    ----------
    db : list
        A list of datasets, each containing exchanges.

    Returns
    -------
        A modified list of datasets with categories as tuples.

    Examples
    --------
    >>> db = [{"categories": ["A", "B"]}, {"categories": ("C", "D")}]
    >>> new_db = ensure_categories_are_tuples(db)
    >>> new_db
    [{"categories": ("A", "B")}, {"categories": ("C", "D")}]
    """
    for ds in db:
        if ds.get("categories") and type(ds["categories"]) != tuple:
            ds["categories"] = tuple(ds["categories"])
    return db
