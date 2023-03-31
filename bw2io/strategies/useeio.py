import random


def remove_useeio_products(data):
    """Remove products from US EEIO and collapse to only activities"""
    products = {ds["code"] for ds in data if ds["type"] == "product"}
    mapping = {
        exc["code"]: ds["code"]
        for ds in data
        for exc in ds["exchanges"]
        if ds["type"] == "process"
        and exc["type"] == "production"
        and exc["code"] in products
    }
    for ds in data:
        for exc in ds.get("exchanges", []):
            db_name, code = exc["input"]
            try:
                exc["input"] = (db_name, mapping[code])
            except KeyError:
                pass
    data = [ds for ds in data if ds["type"] != "product"]
    return data


def remove_random_exchanges(data, fraction=0.9):
    """Remove most inputs to make the US EEIO have a structure more like other LCA databases"""
    for ds in data:
        cutoff = random.triangular(fraction * 0.8, 1, fraction)
        ds["exchanges"] = [
            exc
            for exc in ds["exchanges"]
            if (exc.get("type") not in ("technosphere", "biosphere"))
            or (random.random() > cutoff)
        ]
    return data
