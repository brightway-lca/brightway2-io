def csv_restore_tuples(data):
    """
    Convert tuple-like strings to actual tuples.

    Parameters
    ----------
    data : list of dict
        A list of datasets.

    Returns
    -------
    list of dict
        A list of datasets with tuples restored from string.

    Examples
    --------
    >>> data = [{'categories': 'category1::category2'}, {'exchanges': [{'categories': 'category3::category4', 'amount': '10.0'}]}]
    >>> csv_restore_tuples(data)
    [{'categories': ('category1', 'category2')}, {'exchanges': [{'categories': ('category3', 'category4'), 'amount': '10.0'}]}]

    """
    _ = lambda x: tuple(x.split("::")) if "::" in x else x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
            if key == "categories" and isinstance(ds[key], str):
                ds[key] = (ds[key],)
        for exc in ds.get("exchanges", []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
                if key == "categories" and isinstance(exc[key], str):
                    exc[key] = (exc[key],)
    return data


def csv_restore_booleans(data):
    """
    Convert boolean-like strings to booleans where possible.
    
    Parameters
    ----------
    data : list of dict
        A list of datasets.
        
    Returns
    -------
    list of dict
        A list of datasets with booleans restored.
        
    Examples
    --------
    >>> data = [{'categories': 'category1', 'is_animal': 'true'}, {'exchanges': [{'categories': 'category2', 'amount': '10.0', 'uncertainty type': 'undefined', 'is_biomass': 'False'}]}]
    >>> csv_restore_booleans(data)
    [{'categories': 'category1', 'is_animal': True}, {'exchanges': [{'categories': 'category2', 'amount': '10.0', 'uncertainty type': 'undefined', 'is_biomass': False}]}]
    """
    def _(x):
        if x.lower() == "true":
            return True
        elif x.lower() == "false":
            return False
        else:
            return x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
        for exc in ds.get("exchanges", []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
    return data


def csv_numerize(data):
    """
    Convert string values to float or int where possible

    Parameters
    ----------
    data : list of dict
        A list of datasets.

    Returns
    -------
    list of dict
        A list of datasets with string values converted to float or int where possible.

    Examples
    --------
    >>> data = [{'amount': '10.0'}, {'exchanges': [{'amount': '20', 'uncertainty type': 'undefined'}]}]
    >>> csv_numerize(data)
    [{'amount': 10.0}, {'exchanges': [{'amount': 20, 'uncertainty type': 'undefined'}]}]
    """
    def _(x):
        try:
            return float(x)
        except:
            return x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
        for exc in ds.get("exchanges", []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
    return data


def csv_drop_unknown(data):
    """
    Remove any keys whose values are `(Unknown)`.

    Parameters
    ----------
    data : list[dict]
        A list of dictionaries, where each dictionary represents a row of data.

    Returns
    -------
    list[dict]
        The updated list of dictionaries with `(Unknown)` values removed from the keys.

    Examples
    --------
    >>> data = [
            {"name": "John", "age": 30, "gender": "(Unknown)"},
            {"name": "Alice", "age": 25, "gender": "Female"},
            {"name": "Bob", "age": 40, "gender": "Male"}
        ]
    >>> csv_drop_unknown(data)
        [
            {"name": "Alice", "age": 25, "gender": "Female"},
            {"name": "Bob", "age": 40, "gender": "Male"}
        ]
    """
    _ = lambda x: None if x == "(Unknown)" else x

    data = [{k: v for k, v in ds.items() if v != "(Unknown)"} for ds in data]

    for ds in data:
        if "exchanges" in ds:
            ds["exchanges"] = [
                {k: v for k, v in exc.items() if v != "(Unknown)"}
                for exc in ds["exchanges"]
            ]

    return data


def csv_add_missing_exchanges_section(data):
    """
    Add an empty `exchanges` section to any dictionary in `data` that doesn't already have one.
    
    Parameters
    ----------
    data: list of dict
        A list of dictionaries, where each dictionary represents a row of data.
        
    Returns
    -------
    list[dict]
        The updated list of dictionaries with an empty `exchanges` section added to any dictionary that doesn't already have one.
        
    Examples
    --------
    >>> data = [
            {"name": "John", "age": 30},
            {"name": "Alice", "age": 25, "exchanges": []},
            {"name": "Bob", "age": 40, "exchanges": [{"name": "NYSE"}]}
        ]
    >>> csv_add_missing_exchanges_section(data)
        [
            {"name": "John", "age": 30, "exchanges": []},
            {"name": "Alice", "age": 25, "exchanges": []},
            {"name": "Bob", "age": 40, "exchanges": [{"name": "NYSE"}]}
        ]
    """
    for ds in data:
        if "exchanges" not in ds:
            ds["exchanges"] = []
    return data
