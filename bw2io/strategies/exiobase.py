import csv
import json
import re

from bw2data import Database, config

from ..data import dirpath as data_directory


def normalize_units(data, label="unit"):
    """"
    Normalize the units of the given data by replacing them with their equivalent,
    standardized representations.
    The function takes a list of dictionaries where each dictionary contains a key-value
    pair for the specified label (default: "unit"). The function uses a lookup table to
    replace the unit values with their standardized representations.

    Parameters
    ----------
    data : list
        A list of dictionaries where each dictionary contains a key-value pair for the
        specified label.
    label : str, optional
        The label of the key in the dictionaries whose value needs to be normalized
        (default: "unit").

    Returns
    -------
    list
        The updated list of dictionaries with normalized units.

    Examples
    --------
    >>> data = [
    ...     {"unit": "M.EUR"},
    ...     {"unit": "1000 p"},
    ...     {"unit": "M.hr"},
    ... ]
    >>> normalize_units(data)
    [
        {"unit": "million €"},
        {"unit": "1000 people"},
        {"unit": "million hour"},
    ]
    """
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
    """
    Removes any numeric codes found at the end of the product names
    in the given list of products. 

    Parameters
    ----------
    products : list of dict
        A list of dictionaries, where each dictionary contains a key-value pair
        for the product name.

    Returns
    -------
    list of dict
        The updated list of dictionaries with numeric codes removed from product names.

    Raises
    ------
    TypeError
        If products is not a list of dictionaries.

    Examples
    --------
    >>> products = [
    ...     {"name": "product A (01)"},
    ...     {"name": "product B (99)"},
    ... ]
    >>> remove_numeric_codes(products)
    [
        {"name": "product A"},
        {"name": "product B"},
    ]
    """
    for p in products:
        p["name"] = re.sub(r" \(\d\d\)$", "", p["name"])
    return products


def add_stam_labels(data):
    """
    Adds STAM labels to the input data, which should be a list
    of dictionaries containing a key-value pair for the name. The STAM labels
    are loaded from a JSON file located in the 'lci' directory.

        Parameters
    ----------
    data : list of dict
        A list of dictionaries, where each dictionary contains a key-value pair
        for the name.

    Returns
    -------
    list of dict
        The updated list of dictionaries with added STAM labels.

    Raises
    ------
    TypeError
        If data is not a list of dictionaries.

    Examples
    --------
    >>> data = [
    ...     {"name": "element 1"},
    ...     {"name": "element 2"},
    ... ]
    >>> add_stam_labels(data)
    [
        {"name": "element 1", "stam": "STAM 1"},
        {"name": "element 2", "stam": "STAM 2"},
    ]
    """
    stam = {
        el: stam
        for stam, lst in json.load(
            open(data_directory / "lci" / "EXIOBASE_STAM_categories.json")
        )["data"].items()
        for el in lst
    }
    for obj in data:
        obj["stam"] = stam.get(obj["name"])
    return data


def rename_exiobase_co2_eq_flows(flows):
    """
    Renames CO2 equivalent flows in the input list of flows. The
    input list should contain dictionaries with a key-value pair for the
    'exiobase name'. The function updates the 'exiobase name' for the flows
    that match the provided mapping.

    Parameters
    ----------
    flows : list of dict
        A list of dictionaries, where each dictionary contains a key-value pair
        for the 'exiobase name'.

    Returns
    -------
    list of dict
        The updated list of flows with renamed CO2 equivalent flows.

    Examples
    --------
    >>> flows = [
    ...     {"exiobase name": "PFC - air"},
    ...     {"exiobase name": "HFC - air"},
    ... ]
    >>> rename_exiobase_co2_eq_flows(flows)
    [
        {"exiobase name": "PFC (CO2-eq)"},
        {"exiobase name": "HFC (CO2-eq)"},
    ]
    """
    mapping = {"PFC - air": "PFC (CO2-eq)", "HFC - air": "HFC (CO2-eq)"}
    for flow in flows:
        flow["exiobase name"] = mapping.get(
            flow["exiobase name"], flow["exiobase name"]
        )
    return flows


def get_exiobase_biosphere_correspondence():
    """
    Reads the 'EXIOBASE-ecoinvent-biosphere.csv' file and
    returns the correspondence data as a list of dictionaries. The file is
    expected to be in the 'data_directory/lci' directory.

    Returns
    -------
    list of dict
        A list of dictionaries containing Exiobase biosphere correspondence data.

    Examples
    --------
    >>> correspondence_data = get_exiobase_biosphere_correspondence()
    """
    with open(
        data_directory / "lci" / "EXIOBASE-ecoinvent-biosphere.csv",
        encoding="utf-8-sig",
    ) as f:
        data = [line for line in csv.DictReader(f)]
    return data


def get_categories(x):
    """
    Takes a dictionary containing 'ecoinvent category' and
    'ecoinvent subcategory' keys and returns a tuple containing the category
    and subcategory if both are available, or just the category if the
    subcategory is not available.

    Parameters
    ----------
    x : dict
        A dictionary containing 'ecoinvent category' and 'ecoinvent subcategory'.

    Returns
    -------
    tuple
        A tuple containing the ecoinvent category and subcategory if both are available,
        or just the category if the subcategory is not available.

    Examples
    --------
    >>> data = {'ecoinvent category': 'Energy', 'ecoinvent subcategory': 'Electricity'}
    >>> get_categories(data)
    ('Energy', 'Electricity')
    """
    if x["ecoinvent subcategory"]:
        return (x["ecoinvent category"], x["ecoinvent subcategory"])
    else:
        return (x["ecoinvent category"],)


def add_biosphere_ids(correspondence, biospheres=None):
    """
    Add 'id' key to each dictionary in the list of correspondence data based on the ecoinvent
    and exiobase names found in the specified biosphere databases.

    Parameters
    ----------
    correspondence : list of dict
        A list of dictionaries containing correspondence data.
    biospheres : list, optional
        A list of biosphere database names. Defaults to the biosphere defined in the
        configuration file.

    Returns
    -------
    list of dict
        The correspondence data with added 'id' keys.

    Raises
    ------
    ValueError
        If the correspondence data does not have the required keys, or if a specified
        biosphere database does not exist.

    Examples
    --------
    >>> correspondence_data = [{'ecoinvent name': 'CO2', 'exiobase name': 'CO2', 'ecoinvent category': 'air', 'ecoinvent subcategory': ''}]
    >>> add_biosphere_ids(correspondence_data)
    [{'ecoinvent name': 'CO2', 'exiobase name': 'CO2', 'ecoinvent category': 'air', 'ecoinvent subcategory': '', 'id': some_id}]
    """
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
    """    
    Add 'id' key to each dictionary in the list of products based on the name and location
    of the products found in the specified database.

    Parameters
    ----------
    products : list of dict
        A list of dictionaries containing product data.
    db_name : str
        The name of the database to look up the product IDs.

    Returns
    -------
    list of dict
        The products data with added 'id' keys.

    Raises
    ------
    ValueError
        If the product data does not have the required keys, or if the specified database
        does not exist.

    Examples
    --------
    >>> products_data = [{'name': 'Electricity', 'location': 'CH'}]
    >>> add_product_ids(products_data, 'ecoinvent 3.7.1')
    [{'name': 'Electricity', 'location': 'CH', 'id': some_id}]
    """
    mapping = {(o["name"], o["location"]): o.id for o in Database(db_name)}

    for product in products:
        product["id"] = mapping[(product["name"], product["location"])]

    return products
