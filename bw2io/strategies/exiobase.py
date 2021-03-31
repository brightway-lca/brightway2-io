from ..data import dirpath as data_directory
from bw2data import Database, config
import csv
import json
import re


def normalize_units(data, label="unit"):
    lookup = {
        "M.EUR": "million €",
        "1000 p": "1000 people",
        "M.hr": "million hour",
        "kg": "kilogram",
        "kg CO2-eq": "kilogram CO2-eq.",
        "km2": "square kilometer",
        "TJ": "terajoule",
        "kt": "kilo ton",
        "Mm3": "million cubic meter",
    }
    for o in data:
        o[label] = lookup.get(o[label], o[label])
    return data


def remove_numeric_codes(products):
    for p in products:
        p["name"] = re.sub(r" \(\d\d\)$", "", p["name"])
    return products


def add_stam_labels(data):
    stam = {
        el: stam
        for stam, lst in json.load(
            open(data_directory / "lci" / "EXIOBASE_STAM_categories.json")
        )["data"].items()
        for el in lst
    }
    for obj in data:
        obj["stam"] = stam[obj["name"]]
    return data


def rename_exiobase_co2_eq_flows(flows):
    mapping = {"PFC - air": "PFC (CO2-eq)", "HFC - air": "HFC (CO2-eq)"}
    for flow in flows:
        flow["exiobase name"] = mapping.get(
            flow["exiobase name"], flow["exiobase name"]
        )
    return flows


def get_exiobase_biosphere_correspondence():
    with open(
        data_directory / "lci" / "EXIOBASE-ecoinvent-biosphere.csv",
        encoding="utf-8-sig",
    ) as f:
        data = [line for line in csv.DictReader(f)]
    return data


def get_categories(x):
    if x["ecoinvent subcategory"]:
        return (x["ecoinvent category"], x["ecoinvent subcategory"])
    else:
        return (x["ecoinvent category"],)


def add_biosphere_ids(correspondence, biospheres=None):
    mapping = {}

    if biospheres is None:
        biospheres = [config.biosphere]

    for biosphere in biospheres:
        db = Database(biosphere)
        mapping.update({(o["name"], o["categories"]): o.id for o in db})

    for obj in correspondence:
        if (obj["ecoinvent name"], get_categories(obj)) in mapping:
            obj["id"] = mapping[(obj["ecoinvent name"], get_categories(obj))]
        elif (obj["exiobase name"], get_categories(obj)) in mapping:
            obj["id"] = mapping[(obj["exiobase name"], get_categories(obj))]
        else:
            continue

    return correspondence


def add_product_ids(products, db_name):
    mapping = {(o["name"], o["location"]): o.id for o in Database(db_name)}

    for product in products:
        product["id"] = mapping[(product["name"], product["location"])]

    return products
