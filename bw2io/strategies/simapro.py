import copy
import re

import numpy as np
from bw2data import Database
import bw2parameters
from stats_arrays import LognormalUncertainty

from ..compatibility import (
    SIMAPRO_BIO_SUBCATEGORIES,
    SIMAPRO_BIOSPHERE,
)
from ..data import get_valid_geonames
from ..utils import load_json_data_file, rescale_exchange
from .generic import link_technosphere_by_activity_hash
from .locations import GEO_UPDATE

# Pattern for SimaPro munging of ecoinvent names
detoxify_pattern = "^(?P<name>.+?)/(?P<geo>[A-Za-z]{2,10})(/I)? [SU]$"
detoxify_re = re.compile(detoxify_pattern)


def sp_allocate_products(db):
    """
    Allocate products in a SimaPro dataset by creating a separate dataset for each product.

    For raw SimaPro datasets, creates a separate dataset for each product,
    taking into account the allocation factor if provided. Also handles
    waste treatment datasets with a single product.

    Parameters
    ----------
    db : list
        A list of dictionaries representing raw SimaPro datasets.

    Returns
    -------
    new_db : list
        A list of dictionaries representing the allocated datasets with separate
        entries for each product.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "name": "Dataset 1",
    ...         "exchanges": [
    ...             {"type": "production", "name": "Product A", "unit": "kg", "amount": 10, "allocation": 80},
    ...             {"type": "production", "name": "Product B", "unit": "kg", "amount": 20, "allocation": 20},
    ...         ],
    ...     }
    ... ]
    >>> sp_allocate_products(db)
    [
        {
            "name": "Product A",
            "reference product": "Product A",
            "unit": "kg",
            "production amount": 10,
            "exchanges": [
                {"type": "production", "name": "Product A", "unit": "kg", "amount": 10, "allocation": 80},
                {"type": "production", "name": "Product B", "unit": "kg", "amount": 5, "allocation": 20},
            ],
        },
        {
            "name": "Product B",
            "reference product": "Product B",
            "unit": "kg",
            "production amount": 5,
            "exchanges": [
                {"type": "production", "name": "Product A", "unit": "kg", "amount": 2.5, "allocation": 80},
                {"type": "production", "name": "Product B", "unit": "kg", "amount": 5, "allocation": 20},
            ],
        },
    ]
    """
    new_db = []
    for ds in db:
        products = [
            exc for exc in ds.get("exchanges", []) if exc["type"] == "production"
        ]
        if ds.get("reference product"):
            new_db.append(ds)
        elif not products:
            ds["error"] = True
            new_db.append(ds)
        elif len(products) == 1:
            # Waste treatment datasets only allowed one product
            product = products[0]
            ds["name"] = ds["reference product"] = product["name"]
            ds["unit"] = product["unit"]
            ds["production amount"] = product["amount"]
            new_db.append(ds)
        else:
            ds["exchanges"] = [
                exc for exc in ds["exchanges"] if exc["type"] != "production"
            ]
            for product in products:
                product = copy.deepcopy(product)
                if product["allocation"]:
                    allocation = product["allocation"]
                    if type(product["allocation"]) is str and "parameters" in ds:
                        ds["parameters"] = {
                            k.lower(): v for k, v in ds["parameters"].items()
                        }
                        interp = bw2parameters.ParameterSet(
                            ds["parameters"]
                        ).get_interpreter()
                        interp.add_symbols(
                            bw2parameters.ParameterSet(
                                ds["parameters"]
                            ).evaluate_and_set_amount_field()
                        )
                        allocation = interp(
                            normalize_simapro_formulae(
                                product["allocation"].lower(),
                                settings={"Decimal separator": ","},
                            )
                        )

                    if allocation != 0:
                        product["amount"] = product["amount"] * 1 / (allocation / 100)
                    else:
                        product["amount"] = 0  # Infinity as zero? :-/
                else:
                    product["amount"] = 0
                copied = copy.deepcopy(ds)
                copied["exchanges"].append(product)
                copied["name"] = copied["reference product"] = product["name"]
                copied["unit"] = product["unit"]
                copied["production amount"] = product["amount"]
                new_db.append(copied)
    return new_db


def fix_zero_allocation_products(db):
    """
    Fix datasets with a single production exchange and zero allocation factors.

    For datasets with a single production exchange and zero allocation factors, 
    sets the production amount to one and removes all inputs. This prevents the creation of a singular technosphere matrix.

    Parameters
    ----------
    db : list
        A list of dictionaries representing datasets with production exchanges.

    Returns
    -------
    db : list
        A list of dictionaries representing modified datasets with fixed zero allocation factors.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "name": "Dataset 1",
    ...         "exchanges": [
    ...             {"type": "production", "name": "Product A", "unit": "kg", "amount": 0},
    ...             {"type": "input", "name": "Resource 1", "unit": "kg", "amount": 5},
    ...         ],
    ...     }
    ... ]
    >>> fix_zero_allocation_products(db)
    [
        {
            "name": "Dataset 1",
            "exchanges": [
                {"type": "production", "name": "Product A", "unit": "kg", "amount": 1, "uncertainty type": 0},
            ],
        },
    ]
    """
    for ds in db:
        if (
            len([exc for exc in ds.get("exchanges", []) if exc["type"] == "production"])
            == 1
        ) and all(
            exc["amount"] == 0
            for exc in ds.get("exchanges", [])
            if exc["type"] == "production"
        ):
            ds["exchanges"] = [
                exc for exc in ds["exchanges"] if exc["type"] == "production"
            ]
            exc = ds["exchanges"][0]
            exc["amount"] = exc["loc"] = 1
            exc["uncertainty type"] = 0
    return db


def link_technosphere_based_on_name_unit_location(db, external_db_name=None):
    """
    Link technosphere exchanges based on name, unit, and location.

    Links technosphere exchanges internally or against an external database
    based on their name, unit, and location. It doesn't use categories because categories
    cannot be reliably extracted from SimaPro exports.

    Parameters
    ----------
    db : list
        A list of dictionaries representing datasets with technosphere exchanges.
    external_db_name : str, optional
        The name of the external database to link against, by default None.
        If None, link technosphere exchanges internally within the given database.

    Returns
    -------
    db : list
        A list of dictionaries representing modified datasets with linked technosphere exchanges.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "name": "Dataset 1",
    ...         "exchanges": [
    ...             {"type": "technosphere", "name": "Product A", "unit": "kg", "location": "GLO"},
    ...         ],
    ...     }
    ... ]
    >>> link_technosphere_based_on_name_unit_location(db)
    [
        {
            "name": "Dataset 1",
            "exchanges": [
                {"type": "technosphere", "name": "Product A", "unit": "kg", "location": "GLO"},
            ],
        },
    ]
    """
    return link_technosphere_by_activity_hash(
        db, external_db_name=external_db_name, fields=("name", "location", "unit")
    )


def split_simapro_name_geo(db):
    """
    Split a name like 'foo/CH U' into name and geo components in a dataset.

    Processes datasets and their exchanges by splitting their names
    into name and geo components (e.g., 'foo/CH U' into 'foo' and 'CH U'). The original
    name is stored in a new field called 'simapro name'.

    Parameters
    ----------
    db : list
        A list of dictionaries representing datasets with names to be split.

    Returns
    -------
    db : list
        A list of dictionaries representing modified datasets with split names and geo components.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "name": "foo/CH U",
    ...         "exchanges": [
    ...             {"name": "bar/US U", "type": "technosphere"},
    ...         ],
    ...     }
    ... ]
    >>> split_simapro_name_geo(db)
    [
        {
            "name": "foo",
            "simapro name": "foo/CH U",
            "location": "CH U",
            "exchanges": [
                {"name": "bar", "simapro name": "bar/US U", "location": "US U", "type": "technosphere"},
            ],
        },
    ]
    """
    for ds in db:
        match = detoxify_re.match(ds["name"])
        if match:
            gd = match.groupdict()
            ds["simapro name"] = ds["name"]
            ds["location"] = gd["geo"]
            ds["name"] = ds["reference product"] = gd["name"]
        for exc in ds.get("exchanges", []):
            match = detoxify_re.match(exc["name"])
            if match:
                gd = match.groupdict()
                exc["simapro name"] = exc["name"]
                exc["location"] = gd["geo"]
                exc["name"] = gd["name"]
    return db


def normalize_simapro_biosphere_categories(db):
    """
    Normalize biosphere categories in a dataset to the ecoinvent standard.

    Processes datasets and their exchanges by normalizing biosphere
    categories and subcategories to match the ecoinvent standard. It uses predefined
    mappings for SimaPro and ecoinvent categories.

    Parameters
    ----------
    db : list
        A list of dictionaries representing datasets with biosphere exchanges.

    Returns
    -------
    db : list
        A list of dictionaries representing modified datasets with normalized biosphere categories.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "biosphere",
    ...                 "categories": ["emission", "air"],
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> normalize_simapro_biosphere_categories(db)
    [
        {
            "exchanges": [
                {
                    "type": "biosphere",
                    "categories": ("Emissions", "Air"),
                },
            ],
        },
    ]
    """
    for ds in db:
        for exc in (
            exc for exc in ds.get("exchanges", []) if exc["type"] == "biosphere"
        ):
            cat = SIMAPRO_BIOSPHERE.get(exc["categories"][0], exc["categories"][0])
            if len(exc["categories"]) > 1:
                subcat = SIMAPRO_BIO_SUBCATEGORIES.get(
                    exc["categories"][1], exc["categories"][1]
                )
                exc["categories"] = (cat, subcat)
            else:
                exc["categories"] = (cat,)
    return db


def normalize_simapro_biosphere_names(db):
    """
    Normalize biosphere flow names in a dataset to the ecoinvent standard.

    Processes datasets and their exchanges by normalizing biosphere
    flow names to match the ecoinvent standard. It uses a predefined mapping for
    SimaPro and ecoinvent flow names.

    Parameters
    ----------
    db : list
        A list of dictionaries representing datasets with biosphere exchanges.

    Returns
    -------
    db : list
        A list of dictionaries representing modified datasets with normalized biosphere flow names.

    Examples
    --------
    >>> db = [
    ...     {
    ...         "exchanges": [
    ...             {
    ...                 "type": "biosphere",
    ...                 "categories": ["Emissions", "Air"],
    ...                 "name": "Example emission",
    ...             },
    ...         ],
    ...     }
    ... ]
    >>> normalize_simapro_biosphere_names(db)
    [
        {
            "exchanges": [
                {
                    "type": "biosphere",
                    "categories": ["Emissions", "Air"],
                    "name": "Normalized emission",
                },
            ],
        },
    ]
    """
    mapping = {tuple(x[:2]): x[2] for x in load_json_data_file("simapro-biosphere")}
    for ds in db:
        for exc in (
            exc for exc in ds.get("exchanges", []) if exc["type"] == "biosphere"
        ):
            try:
                exc["name"] = mapping[(exc["categories"][0], exc["name"])]
            except KeyError:
                pass
    return db


iff_exp = re.compile(
    "iff\("  # Starting condition, case-insensitive
    "\s*"  # Whitespace
    "(?P<condition>[^,]+)"  # Anything except a comma (not sure what else could go here, so capture everything)
    "\s*"  # Whitespace
    ","  # Comma marks the end of the conditional clause
    "\s*"  # Whitespace
    "(?P<when_true>[^,]+)"  # Value if condition is true
    "\s*"  # Whitespace
    ","  # Comma marks the end of the true value clause
    "\s*"  # Whitespace
    "(?P<when_false>[^,]+)"  # Value if condition is false
    "\s*"  # Whitespace
    "\)",  # End parentheses
    re.IGNORECASE,
)


def fix_iff_formula(string):
    """
    Replace SimaPro 'iff' formula with a Python equivalent 'if-else' expression.

    Processes a given string containing SimaPro 'iff' formulae and
    replaces them with Python equivalent 'if-else' expressions. The conversion
    is done using regular expressions.

    Parameters
    ----------
    string : str
        A string containing SimaPro 'iff' formulae.

    Returns
    -------
    string : str
        A string with SimaPro 'iff' formulae replaced by Python 'if-else' expressions.

    Examples
    --------
    >>> string = "iff(A > 0, A, 0)"
    >>> fix_iff_formula(string)
    "((A) if (A > 0) else (0))"
    """
    while iff_exp.findall(string):
        match = next(iff_exp.finditer(string))
        string = (
            string[: match.start()]
            + "(({when_true}) if ({condition}) else ({when_false}))".format(
                **match.groupdict()
            )
            + string[match.end() :]
        )
    return string


def normalize_simapro_formulae(formula, settings):
    """
    Convert SimaPro formulae to Python expressions.

    Processes a given formula string containing SimaPro formulae
    and converts them to Python expressions. The conversion is done using
    string manipulation and by calling the `fix_iff_formula` function.

    Parameters
    ----------
    formula : str
        A string containing SimaPro formulae.
    settings : dict
        A dictionary containing settings that affect the formula conversion,
        e.g., decimal separator.

    Returns
    -------
    str
        A string with SimaPro formulae replaced by equivalent Python expressions.

    Examples
    --------
    >>> formula = "A^2"
    >>> settings = {"Decimal separator": ","}
    >>> normalize_simapro_formulae(formula, settings)
    "A**2"
    """

    def replace_comma(match):
        return match.group(0).replace(",", ".")

    formula = formula.replace("^", "**")
    if settings and settings.get("Decimal separator") == ",":
        formula = re.sub("\d,\d", replace_comma, formula)
    formula = fix_iff_formula(formula)
    return formula


def change_electricity_unit_mj_to_kwh(db):
    """
    Change datasets with the string "electricity" in their name from units of MJ to kilowatt hour.

    Iterates through a given database (list of datasets) and modifies the unit of exchanges
    containing the string "electricity" or "market for electricity" in their name from "megajoule" (MJ) to
    "kilowatt hour" (kWh). It also rescales the exchange accordingly.

    Parameters
    ----------
    db : list
        A list of datasets containing exchanges with the unit "megajoule" (MJ).

    Returns
    -------
    list
        A modified list of datasets with exchanges containing the string "electricity" or
        "market for electricity" in their name updated to have the unit "kilowatt hour" (kWh).

    Examples
    --------
    >>> db = [
            {
                "exchanges": [
                    {"name": "Electricity", "unit": "megajoule", "amount": 3.6}
                ]
            }
        ]
    >>> change_electricity_unit_mj_to_kwh(db)
    [{'exchanges': [{'name': 'Electricity', 'unit': 'kilowatt hour', 'amount': 1.0}]}]
    """
    for ds in db:
        for exc in ds.get("exchanges", []):
            if (
                exc.get("name", "").lower().startswith("electricity")
                or exc.get("name", "").lower().startswith("market for electricity")
            ) and exc.get("unit") == "megajoule":
                exc["unit"] = "kilowatt hour"
                rescale_exchange(exc, 1 / 3.6)
    return db


def fix_localized_water_flows(db):
    """
    Change water flows with location information to generic water flows.

    Biosphere flows cannot have locations; locations are defined by the activity dataset.
    Iterates through a given database (list of datasets) and modifies the name of
    exchanges containing water flows with location information by removing the location details.

    Parameters
    ----------
    db : list
        A list of datasets containing exchanges with water flows including location information.

    Returns
    -------
    list
        A modified list of datasets with exchanges containing water flows updated to have generic names,
        without location information.

    Examples
    --------
    >>> db = [
            {
                "exchanges": [
                    {"name": "Water, river, BR", "type": "biosphere"}
                ]
            }
        ]
    >>> fix_localized_water_flows(db)
    [{'exchanges': [{'name': 'Water, river', 'type': 'biosphere', 'simapro location': 'BR'}]}]
    """
    locations = (
        set(get_valid_geonames())
        .union(set(GEO_UPDATE.keys()))
        .union(set(GEO_UPDATE.values()))
    )

    flows = [
        "Water",
        "Water, cooling, unspecified natural origin",
        "Water, river",
        "Water, turbine use, unspecified natural origin",
        "Water, unspecified natural origin",
        "Water, well, in ground",
    ]

    mapping = {
        "{}, {}".format(flow, location): (flow, location)
        for flow in flows
        for location in locations
    }

    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc.get("input") or not exc["type"] == "biosphere":
                continue
            try:
                flow, location = mapping[exc["name"]]
                exc["name"] = flow
                exc["simapro location"] = GEO_UPDATE.get(location, location)
            except KeyError:
                pass
    return db


def set_lognormal_loc_value_uncertainty_safe(db):
    """
    Ensure the 'loc' value is correct for lognormal uncertainty distributions in the given database.

    Iterates through a given database (list of datasets) and updates the 'loc' value
    of exchanges with lognormal uncertainty distributions, setting it to the natural logarithm of
    the absolute value of the exchange amount.

    Parameters
    ----------
    db : list
        A list of datasets containing exchanges with lognormal uncertainty distributions.

    Returns
    -------
    list
        A modified list of datasets with the 'loc' value updated for exchanges with lognormal
        uncertainty distributions.

    Examples
    --------
    >>> db = [
            {
                "exchanges": [
                    {
                        "amount": 10,
                        "uncertainty type": LognormalUncertainty.id,
                        "loc": 0
                    }
                ]
            }
        ]
    >>> set_lognormal_loc_value_uncertainty_safe(db)
    [{'exchanges': [{'amount': 10, 'uncertainty type': 2, 'loc': 2.302585092994046}]}]
    """
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc.get("uncertainty type") == LognormalUncertainty.id:
                exc["loc"] = np.log(abs(exc["amount"]))
    return db


def flip_sign_on_waste(db, other):
    """
    Flip the sign on waste exchanges in the imported database based on the waste convention.

    Adjusts the sign of waste exchanges in the imported database
    to match the waste exchange convention in SimaPro.

    Parameters
    ----------
    db : list
        A list of datasets containing waste exchanges to be adjusted.
    other : str
        The name of the external database (e.g., ecoinvent) that is linked to
        the imported database.

    Returns
    -------
    list
        A modified list of datasets with the sign of waste exchanges updated.

    Notes
    -----
    This strategy needs to be run *after* matching with ecoinvent.
    The strategy should be run as follows:
    sp_imported.apply_strategy(functools.partial(flip_sign_on_waste, other="name_of_other"))

    Examples
    --------
    >>> db = [
            {
                "exchanges": [
                    {
                        "amount": -10,
                        "input": ("key",),
                        "uncertainty type": 0,
                        "loc": -10
                    }
                ]
            }
        ]
    >>> other_db_name = "name_of_other"
    >>> flip_sign_on_waste(db, other_db_name)
    [{'exchanges': [{'amount': 10, 'input': ('key',), 'uncertainty type': 0, 'loc': 10}]}]
    """
    flip_needed = {
        ds.key for ds in Database(other) if ds.get("production amount", 0) < 0
    }
    for ds in db:
        for exc in ds.get("exchanges", []):
            if exc["input"] in flip_needed:
                uncertainty_type = exc.get("uncertainty type")
                if uncertainty_type in [0, 1, 3]:
                    exc["amount"] *= -1
                    exc["loc"] = exc["amount"]
                elif uncertainty_type == 2:
                    exc["amount"] *= -1
                    exc["negative"] = True
                elif uncertainty_type in [4, 5]:
                    exc["amount"] *= -1
                    new_min = -exc["maximum"]
                    new_max = -exc["minimum"]
                    exc["maximum"] = new_max
                    exc["minimum"] = new_min
                    if uncertainty_type == 5:
                        exc["loc"] = exc["amount"]
    return db
