from pprint import pformat
from typing import List
from uuid import uuid4

import bw2data as bd

EDGE_CORE_COLUMNS = [
    "name",
    "amount",
    "database",
    "location",
    "unit",
    "functional",
    "type",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
]


def create_products_as_new_nodes(data: List[dict]) -> List[dict]:
    """
    Create new product nodes and link to them if needed.

    We create new `product` if the following conditions are met:

    * The dataset is not multifunctional (
        `dataset.get("type") != bd.labels.multifunctional_node_default`). Multifunctional datasets
        handle product creation separately.
    * The edge is functional (`obj.get("functional") is True`)
    * The edge is unlinked (`obj.get("input")` is falsey)
    * The given edge has a `name`, and that `name` is different than the dataset `name`
    * The combination of `name` and `location` is not present in the other dataset nodes. If no
        `location` attribute is given for the edge under consideration, we use the `location` of the
        dataset.

    Create new nodes, and links the originating edges to the new product nodes.

    Modifies data in-place, and returns the modified `data`.

    """
    combos = {(ds.get("name"), ds.get("location")) for ds in data}
    nodes = []

    for ds in data:
        if ds.get("type") == bd.labels.multifunctional_node_default:
            # Has its own product handling
            continue
        for edge in ds.get("exchanges", []):
            if (
                edge.get("functional")
                and not edge.get("input")
                and edge.get("name")
                and edge["name"] != ds.get("name")
            ):
                if not ds.get("database"):
                    raise KeyError(
                        """
Can't create a new `product` node, as dataset is missing `database` attribute:
{}""".format(
                            pformat(ds)
                        )
                    )
                key = (edge["name"], edge.get("location") or ds.get("location"))
                if key not in combos:
                    code = uuid4().hex
                    nodes.append(
                        {
                            "name": edge["name"],
                            "location": key[1] or bd.config.global_location,
                            "unit": edge.get("unit") or ds.get("unit"),
                            "exchanges": [],
                            "code": code,
                            "type": bd.labels.product_node_default,
                            "database": ds["database"],
                        }
                        | {k: v for k, v in edge.items() if k not in EDGE_CORE_COLUMNS}
                    )
                    edge["input"] = (ds["database"], code)
                    combos.add(key)

    if nodes:
        data.extend(nodes)
    return data
