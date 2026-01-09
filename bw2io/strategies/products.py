from pprint import pformat
from typing import List
from uuid import uuid4

import bw2data as bd
from bw2data.logs import stdout_feedback_logger

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


def separate_processes_from_products(
    data: List[dict], field_exclusions=["location"], code_suffix: str = "-product"
) -> List[dict]:
    """Given a set of processes, and no separate data on products, create copies of the processes
    as products and re-link the local supply chain.

    Designed for use in importing databases where processes are not strongly typed as different from
    products.

    Copies over all attributes from the source processes except:

    - Those listed in `field_exclusions`
    - `type` is set to `bw2data.labels.product_node_default`
    - Uses the `code_suffix` to generate a new `code` value (previous plus code suffix)
    - No edges are copied over
    - If the attribute is present `reference product`, this is used as the product name

    Should come late in the import process, when internal links are all present."""
    if bd.labels.product_node_default in {ds.get("type") for ds in data}:
        raise ValueError(
            "This function requires no product nodes in the imported database"
        )

    processes = [
        ds
        for ds in data
        if ds.get("type")
        in (bd.labels.process_node_default, bd.labels.chimaera_node_default)
    ]
    codes = {ds["code"] for ds in processes}
    if intersection := {code + code_suffix for code in codes}.intersection(codes):
        raise ValueError(
            f"Given `code_suffix` results in code overlaps for the following process codes: {intersection}"
        )

    product_mapping, products = {}, []

    for ds in processes:
        if not ds.get("exchanges"):
            stdout_feedback_logger.warning(
                f"Skipping dataset {ds.get('name')} | `{ds['code']}` with no edges"
            )
        self_production = [
            exc
            for exc in ds["exchanges"]
            if exc.get("input") == (ds.get("database"), ds["code"])
            and (
                exc.get("type") == bd.labels.production_edge_default
                or exc.get("functional")
            )
        ]
        if len(self_production) > 1:
            stdout_feedback_logger.info(
                f"Process dataset {ds.get('name')} | `{ds['code']}` has {len(self_production)} functional edges which will all be linked to a new product"
            )
        elif not self_production:
            stdout_feedback_logger.warning(
                f"Skipping process dataset {ds.get('name')} | `{ds['code']}` as it has no self-referential production edges"
            )
            continue

        product = {
            "type": bd.labels.product_node_default,
            "exchanges": [],
            "code": ds["code"] + code_suffix,
            "database": ds["database"],
        }
        product_mapping[(ds['database'], ds["code"])] = (ds['database'], product["code"])
        for key, value in ds.items():
            if key.lower() in ("product", "reference_product", "reference product"):
                product["name"] = value
            if key in field_exclusions or key in (
                "exchanges",
                "code",
                "database",
                "type",
            ):
                continue
            elif key == "name" and product.get("name"):
                continue
            else:
                product[key] = value
        products.append(product)
        ds['type'] = bd.labels.process_node_default

    for ds in data:
        for edge in ds.get("exchanges", []):
            if edge.get("input"):
                edge["input"] = product_mapping.get(edge["input"], edge["input"])

    return data + products
