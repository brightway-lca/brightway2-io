import copy


def delete_integer_codes(data):
    """
    Delete integer codes from the input data dictionary.
    
    Parameters
    ----------
    data : list[dict]
        A list of dictionaries, where each dictionary represents a row of data.
        Each dictionary should have a `name` key, and optionally a `code` and or `exchanges` key.
        
    Returns
    -------
    list[dict]
        The updated list of dictionaries with any integer `code` keys removed from
        the dictionaries and their `exchanges` keys
        
    Examples
    --------
    >>> data = [{'name': 'test', 'code': 1}, {'name': 'test2', 'exchanges': [{'code': 2}]}]
    >>> delete_integer_codes(data)
    >>> data == [{'name': 'test'}, {'name': 'test2', 'exchanges': [{}]}]
    """
    for ds in data:
        if "code" in ds and isinstance(ds["code"], int):
            del ds["code"]
        for exc in ds.get("exchanges", []):
            if "code" in exc and isinstance(exc["code"], int):
                del exc["code"]
    return data


def clean_integer_codes(data):
    """
    Convert integer activity codes to strings and delete integer codes from exchanges. 
    
    Parameters
    ----------
    data : list of dict
        List of datasets, where each dataset is a dictionary containing information about the dataset, such as its name,
        description, and exchanges.
        
    Returns
    -------
    list of dict
        The cleaned list of datasets, where integer activity codes have been converted to strings and integer codes
        have been deleted from exchanges.
        
    Examples
    --------
    >>> data = [{'name': 'Dataset A', 'description': '...', 'code': 123},
    ...         {'name': 'Dataset B', 'description': '...', 'exchanges': [{'code': 456, 'amount': 1.0}]}]
    >>> clean_integer_codes(data)
    [{'name': 'Dataset A', 'description': '...', 'code': '123'},
     {'name': 'Dataset B', 'description': '...', 'exchanges': [{'amount': 1.0}]}]
    """
    for ds in data:
        if "code" in ds and isinstance(ds["code"], int):
            ds["code"] = str(ds["code"])
        for exc in ds.get("exchanges", []):
            if "code" in exc and isinstance(exc["code"], int):
                del exc["code"]
    return data


def es1_allocate_multioutput(data):
    """
    This strategy allocates multioutput datasets to new datasets.

    This deletes the multioutput dataset, breaking any existing linking. This shouldn't be a concern, as you shouldn't link to a multioutput dataset in any case.

    Note that multiple allocations for the same product and input will result in undefined behavior.
    
    Parameters
    ----------
    data : list of dict
        List of datasets, where each dataset is a dictionary containing information about the dataset, such as its name,
        description, and exchanges.
        
    Returns
    -------
    list of dict
        The new list of datasets, where multioutput datasets have been allocated to new datasets.

    Examples
    -------
    >>> data = [{'name': 'Dataset A', 'exchanges': [{'name': 'Output 1', 'amount': 1.0},
    ...                                             {'name': 'Output 2', 'amount': 2.0}],
    ...          'allocations': [{'name': 'Activity 1', 'product': 'Output 1', 'input': 'Input 1'},
    ...                           {'name': 'Activity 2', 'product': 'Output 2', 'input': 'Input 2'}]},
    ...         {'name': 'Dataset B', 'exchanges': [{'name': 'Output 1', 'amount': 1.0}],
    ...          'allocations': [{'name': 'Activity 3', 'product': 'Output 1', 'input': 'Input 3'}]}]
    >>> es1_allocate_multioutput(data)
    [{'name': 'Dataset A: Output 1', 'exchanges': [{'name': 'Output 1', 'amount': 1.0}],
      'allocations': [{'name': 'Activity 1', 'product': 'Output 1', 'input': 'Input 1'}]},
     {'name': 'Dataset A: Output 2', 'exchanges': [{'name': 'Output 2', 'amount': 2.0}],
      'allocations': [{'name': 'Activity 2', 'product': 'Output 2', 'input': 'Input 2'}]},
     {'name': 'Dataset B', 'exchanges': [{'name': 'Output 1', 'amount': 1.0}],
      'allocations': [{'name': 'Activity 3', 'product': 'Output 1', 'input': 'Input 3'}]}]
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
    
    Parameters
    ----------
    ds : dict
        A dataset that has multiple outputs.

    Returns
    -------
    list of dict
        A list of allocated datasets.

    Examples
    --------
    >>> ds = {'name': 'Dataset A', 'exchanges': [{'name': 'Output 1', 'code': 1, 'type': 'production'},
    ...                                          {'name': 'Output 2', 'code': 2},
    ...                                          {'name': 'Output 3', 'code': 3}],
    ...       'allocations': [{'exchanges': [2], 'fraction': 50.0, 'reference': 1},
    ...                        {'exchanges': [3], 'fraction': 50.0, 'reference': 1}]}
    >>> allocate_exchanges(ds)
    [{'name': 'Dataset A', 'exchanges': [{'name': 'Output 1', 'code': 1, 'type': 'production'},
                                         {'name': 'Output 2', 'code': 2, 'type': 'from'},
                                         {'name': 'Output 3', 'code': 3, 'type': 'from'}]},
     {'name': 'Dataset A: Output 2', 'exchanges': [{'name': 'Output 2', 'code': 2, 'type': 'production'}],
      'allocations': [{'exchanges': [2], 'fraction': 100.0, 'reference': 2}]},
     {'name': 'Dataset A: Output 3', 'exchanges': [{'name': 'Output 3', 'code': 3, 'type': 'production'}],
      'allocations': [{'exchanges': [3], 'fraction': 100.0, 'reference': 3}]}]
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
    """
    Rescale an exchange by a given factor.

    Parameters
    ----------
    exc : dict
        The exchange to be rescaled.
    scale : float
        The factor by which to rescale the exchange.

    Returns
    -------
    dict
        The rescaled exchange.

    Examples
    --------
    >>> exc = {'name': 'Output 1', 'amount': 1.0}
    >>> rescale_exchange(exc, 2.0)
    {'name': 'Output 1', 'amount': 2.0}
    """
    exc = copy.deepcopy(exc)
    exc["amount"] *= scale
    return exc
