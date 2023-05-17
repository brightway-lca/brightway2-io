from collections import defaultdict
from copy import deepcopy

from ..errors import UnallocatableDataset
from ..utils import rescale_exchange

VALID_METHODS = {
    "PHYSICAL_ALLOCATION",
    "ECONOMIC_ALLOCATION",
    "CAUSAL_ALLOCATION",
    "USE_DEFAULT_ALLOCATION",
    "NO_ALLOCATION",
}


def allocation_needed(ds):
    """
    Determine if allocation is needed for the given dataset.

    Checks if the input dataset requires allocation by examining its
    'allocationFactors' attribute and verifying that its '@type' is neither 'product'
    nor 'emission'. Allocation is typically necessary for datasets that represent
    shared processes or resources that need to be divided among multiple consumers.

    Parameters
    ----------
    ds : dict
        A dictionary representing a dataset, containing keys such as
        'allocationFactors' and '@type'.

    Returns
    -------
    bool
        True if the dataset requires allocation, False otherwise.

    Examples
    --------
    >>> ds_product = {"@type": "product", "allocationFactors": {"A": 0.5, "B": 0.5}}
    >>> allocation_needed(ds_product)
    False

    >>> ds_emission = {"@type": "emission", "allocationFactors": {"A": 0.5, "B": 0.5}}
    >>> allocation_needed(ds_emission)
    False

    >>> ds_shared = {"@type": "shared_process", "allocationFactors": {"A": 0.3, "B": 0.7}}
    >>> allocation_needed(ds_shared)
    True
    """
    return ds.get("allocationFactors") and (ds["@type"] not in ("product", "emission"))


def allocatable_exchanges(exchanges):
    """
    Filter a list of exchanges to retain only those that are allocatable.

    Filters the input list of exchanges, retaining only those that meet
    at least one of the following conditions:
    1. Their 'flowType' attribute in the 'flow' dictionary is 'ELEMENTARY_FLOW'.
    2. Their 'flowType' attribute in the 'flow' dictionary is 'WASTE_FLOW'.
    3. They have an 'avoidedProduct' key.
    4. They have an 'input' key.

    Parameters
    ----------
    exchanges : list of dict
        A list of dictionaries representing exchanges, each containing keys such as
        'flow', 'avoidedProduct', and 'input'.

    Returns
    -------
    list of dict
        A list of allocatable exchanges, filtered based on the specified conditions.

    Examples
    --------
    >>> exchanges = [
    ...     {"flow": {"flowType": "ELEMENTARY_FLOW"}},
    ...     {"flow": {"flowType": "WASTE_FLOW"}},
    ...     {"avoidedProduct": True},
    ...     {"input": "some_input"},
    ...     {"flow": {"flowType": "OTHER_FLOW"}},
    ... ]
    >>> allocatable_exchanges(exchanges)
    [
        {"flow": {"flowType": "ELEMENTARY_FLOW"}},
        {"flow": {"flowType": "WASTE_FLOW"}},
        {"avoidedProduct": True},
        {"input": "some_input"},
    ]
    """
    return [
        exc
        for exc in exchanges
        if (exc.get("flow", {}).get("flowType") == "ELEMENTARY_FLOW")
        or (exc.get("flow", {}).get("flowType") == "WASTE_FLOW")
        or exc.get("avoidedProduct")
        or exc["input"]
    ]


def get_allocation_dict(factors):
    """
    Create a nested dictionary for the given allocation factors.

    Processes a list of allocation factors and organizes them into a
    nested dictionary structure. The outer dictionary has keys corresponding to the
    allocation types ('CAUSAL_ALLOCATION' or other types), while the inner
    dictionaries have keys corresponding to product IDs and, for 'CAUSAL_ALLOCATION',
    also flow IDs.

    Parameters
    ----------
    factors : list of dict
        A list of dictionaries representing allocation factors, each containing keys
        such as 'allocationType', 'product', 'exchange', and 'value'.

    Returns
    -------
    defaultdict(dict)
        A nested dictionary containing the allocation factors organized by allocation
        type, product ID, and, for 'CAUSAL_ALLOCATION', flow ID.

    Raises
    ------
    UnallocatableDataset
        If a 'CAUSAL_ALLOCATION' factor is missing either the 'product' or 'flow' keys.

    Examples
    --------
    >>> factors = [
    ...     {
    ...         "allocationType": "CAUSAL_ALLOCATION",
    ...         "product": {"@id": "P1"},
    ...         "exchange": {"flow": {"@id": "F1"}},
    ...         "value": 0.7,
    ...     },
    ...     {
    ...         "allocationType": "ECONOMIC_ALLOCATION",
    ...         "product": {"@id": "P2"},
    ...         "value": 0.6,
    ...     },
    ... ]
    >>> get_allocation_dict(factors)
    defaultdict(
        dict,
        {
            'CAUSAL_ALLOCATION': {'P1': {'F1': 0.7}},
            'ECONOMIC_ALLOCATION': {'P2': 0.6},
        },
    )
    """
    ad = defaultdict(dict)

    for factor in factors:
        if factor["allocationType"] == "CAUSAL_ALLOCATION":
            try:
                product = factor["product"]["@id"]
                flow = factor["exchange"]["flow"]["@id"]
            except KeyError:
                raise UnallocatableDataset(
                    "We currently only support exchange-specific CAUSAL_ALLOCATION"
                )
            if product not in ad["CAUSAL_ALLOCATION"]:
                ad["CAUSAL_ALLOCATION"][product] = {}
            ad["CAUSAL_ALLOCATION"][product][flow] = factor["value"]
        else:
            ad[factor["allocationType"]][factor["product"]["@id"]] = factor["value"]

    return ad


def get_production_exchanges(exchanges):
    """
    Filter a list of exchanges to retain only those representing production flows.

    Filters the input list of exchanges, retaining only those that have
    a 'flowType' attribute of 'PRODUCT_FLOW' in the 'flow' dictionary and do not have
    an 'input' key. Production flows typically represent the output of a process or
    system.

    Parameters
    ----------
    exchanges : list of dict
        A list of dictionaries representing exchanges, each containing keys such as
        'flow' and 'input'.

    Returns
    -------
    list of dict
        A list of production exchanges, filtered based on the specified conditions.

    Examples
    --------
    >>> exchanges = [
    ...     {"flow": {"flowType": "PRODUCT_FLOW"}},
    ...     {"flow": {"flowType": "PRODUCT_FLOW"}, "input": "some_input"},
    ...     {"flow": {"flowType": "ELEMENTARY_FLOW"}},
    ... ]
    >>> get_production_exchanges(exchanges)
    [{"flow": {"flowType": "PRODUCT_FLOW"}}]
    """
    return [
        exc
        for exc in exchanges
        if exc["flow"]["flowType"] == "PRODUCT_FLOW" and not exc["input"]
    ]


def get_production_exchange(exchanges, flow_id):
    """
    Find the production exchange with the specified flow ID.
    
    Searches the input list of exchanges for the production exchange
    with the specified flow ID. It raises an UnallocatableDataset exception if more
    than one candidate is found or a ValueError if no candidate is found.
    
    Parameters
    ----------
    exchanges : list of dict
        A list of dictionaries representing exchanges, each containing keys such as
        'flow' and 'input'.
    flow_id : str
        The flow ID to search for in the production exchanges.

    Returns
    -------
    dict
        The production exchange with the specified flow ID.

    Raises
    ------
    UnallocatableDataset
        If more than one production exchange with the specified flow ID is found.
    ValueError
        If no production exchange with the specified flow ID is found.

    Examples
    --------
    >>> exchanges = [
    ...     {"flow": {"flowType": "PRODUCT_FLOW", "@id": "F1"}},
    ...     {"flow": {"flowType": "PRODUCT_FLOW", "@id": "F2"}},
    ...     {"flow": {"flowType": "ELEMENTARY_FLOW", "@id": "F3"}},
    ... ]
    >>> get_production_exchange(exchanges, "F1")
    {"flow": {"flowType": "PRODUCT_FLOW", "@id": "F1"}}
    """
    candidates = [
        exc
        for exc in get_production_exchanges(exchanges)
        if exc["flow"]["@id"] == flow_id
    ]
    if len(candidates) > 1:
        raise UnallocatableDataset(
            "Can't uniquely identify the production exchange, and ``internalId`` fields are not reliable"
        )
    elif not candidates:
        raise ValueError("Can't find production exchange for this `flow_id`")
    return candidates[0]


def causal_allocation(exchanges, ad):
    """
    Allocate causal factors to exchanges in a list, applying a rescaling factor based on a given dictionary.

    Iterates over a list of exchanges and applies the corresponding causal allocation factor
    provided in the `ad` dictionary. It raises an UnallocatableDataset exception if the causal allocation
    factor for a given exchange is missing in the `ad` dictionary.

    Parameters
    ----------
    exchanges : list
        A list of dictionaries representing exchanges, where each dictionary contains a flow with an @id key.
    ad : dict
        A dictionary containing causal allocation factors for each exchange, indexed by the exchange flow @id.

    Returns
    -------
    list
        A list of processed exchanges with the causal allocation factors applied.

    Raises
    ------
    UnallocatableDataset
        If the causal allocation factor for an exchange is missing in the `ad` dictionary.

    Examples
    --------
    >>> exchanges = [{'flow': {'@id': 1}, 'amount': 10}, {'flow': {'@id': 2}, 'amount': 20}]
    >>> ad = {1: 0.5, 2: 0.25}
    >>> causal_allocation(exchanges, ad)
    [{'flow': {'@id': 1}, 'amount': 5.0}, {'flow': {'@id': 2}, 'amount': 5.0}]
    """
    processed = []
    for exc in exchanges:
        try:
            exc = rescale_exchange(exc, ad[exc["flow"]["@id"]])
        except KeyError:
            raise UnallocatableDataset(
                "Missing causal allocation factor for exchange: {}".format(exc)
            )
        processed.append(exc)
    return processed


def json_ld_allocate_datasets(db, preferred_allocation=None):
    """
    Perform allocation on multifunctional datasets in a given database according to the specified allocation method.

    Uses the preferred_allocation method if available; otherwise, it uses the default method.
    The allocation is performed according to the JSON-LD specification which lists the following methods:

    * PHYSICAL_ALLOCATION
    * ECONOMIC_ALLOCATION
    * CAUSAL_ALLOCATION (Can be exchange-specific)
    * USE_DEFAULT_ALLOCATION
    * NO_ALLOCATION

    Parameters
    ----------
    db : dict
        A dictionary representing a database containing processes and their exchanges.
    preferred_allocation : str, optional
        The preferred allocation method to use, if available. Defaults to None.

    Returns
    -------
    dict
        A dictionary representing the modified database with allocated datasets.

    Raises
    ------
    AssertionError
        If an invalid allocation method is provided.
    UnallocatableDataset
        If the default allocation is chosen, but allocation factors for this method are not provided.

    Examples
    --------
    >>> db = {
    ...     "processes": {
    ...         # Add processes with exchanges and allocation factors here
    ...     }
    ... }
    >>> preferred_allocation = "ECONOMIC_ALLOCATION"
    >>> json_ld_allocate_datasets(db, preferred_allocation)
    # Returns the modified database with allocated datasets
    """
    if preferred_allocation is not None:
        assert preferred_allocation in VALID_METHODS, "Invalid allocation method given"

    new_datasets = {}

    for ds in db["processes"].values():
        if not allocation_needed(ds):
            continue
        allocation_dict = get_allocation_dict(ds["allocationFactors"])
        allocation_method = (
            preferred_allocation
            if preferred_allocation in allocation_dict
            else ds["defaultAllocationMethod"]
        )

        if (
            preferred_allocation is None
            and ds["defaultAllocationMethod"] not in allocation_dict
        ):
            raise UnallocatableDataset(
                "Default allocation chosen, but allocation factors for this method not provided"
            )

        for prod_exchange in get_production_exchanges(ds["exchanges"]):
            if not prod_exchange["amount"]:
                # Would cause singular matrix
                continue

            new_ds = deepcopy(ds)
            new_ds["code"] = "{}.{}".format(
                new_ds.pop("@id"), prod_exchange["flow"]["@id"]
            )
            prod_exchange["quantitativeReference"] = True
            if allocation_method == "CAUSAL_ALLOCATION":
                new_ds["exchanges"] = [prod_exchange] + causal_allocation(
                    allocatable_exchanges(new_ds["exchanges"]),
                    allocation_dict[allocation_method][prod_exchange["flow"]["@id"]],
                )
            else:
                new_ds["exchanges"] = [prod_exchange] + [
                    rescale_exchange(
                        exc,
                        allocation_dict[allocation_method][
                            prod_exchange["flow"]["@id"]
                        ],
                    )
                    for exc in allocatable_exchanges(new_ds["exchanges"])
                ]
            new_ds["allocationFactors"] = []
            new_datasets[new_ds["code"]] = new_ds

    unallocated = {
        key: value
        for key, value in db["processes"].items()
        if not allocation_needed(value)
    }
    db["processes"] = {**unallocated, **new_datasets}

    return db
