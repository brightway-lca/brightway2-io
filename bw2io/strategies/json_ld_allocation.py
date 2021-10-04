from ..errors import UnallocatableDataset
from ..utils import rescale_exchange
from collections import defaultdict
from copy import deepcopy


VALID_METHODS = {
    "PHYSICAL_ALLOCATION",
    "ECONOMIC_ALLOCATION",
    "CAUSAL_ALLOCATION",
    "USE_DEFAULT_ALLOCATION",
    "NO_ALLOCATION",
}


def allocation_needed(ds):
    return ds.get("allocationFactors") and (ds['@type'] not in ('product', 'emission'))


def allocatable_exchanges(exchanges):
    return [
        exc
        for exc in exchanges
        if (exc.get("flow", {}).get("flowType") == "ELEMENTARY_FLOW")
        or (exc.get("flow", {}).get("flowType") == "WASTE_FLOW")
        or exc.get("avoidedProduct")
        or exc["input"]
    ]


def get_allocation_dict(factors):
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
    return [
        exc
        for exc in exchanges
        if exc["flow"]["flowType"] == "PRODUCT_FLOW" and not exc["input"]
    ]


def get_production_exchange(exchanges, flow_id):
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
    processed = []
    for exc in exchanges:
        try:
            exc = rescale_exchange(exc, ad[exc['flow']['@id']])
        except KeyError:
            raise UnallocatableDataset("Missing causal allocation factor for exchange: {}".format(exc))
        processed.append(exc)
    return processed


def json_ld_allocate_datasets(db, preferred_allocation=None):
    """Perform allocation on multifunctional datasets.

    Uses the ``preferred_allocation`` method if available; otherwise, the default method.

    Here are the allocation methods listed in the JSON-LD spec:

    * PHYSICAL_ALLOCATION
    * ECONOMIC_ALLOCATION
    * CAUSAL_ALLOCATION (Can be exchange-specific)
    * USE_DEFAULT_ALLOCATION
    * NO_ALLOCATION

    We can't use ``@id`` values as codes after allocation, so we combine the process id and the flow id for the allocated dataset.

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
