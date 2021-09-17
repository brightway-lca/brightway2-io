from ..units import normalize_units as normalize_units_function


def json_ld_get_normalized_exchange_locations(data):
    """The exchanges location strings are not necessarily the same as those given in the process or the master metadata. Fix this inconsistency.

    This has to happen before we transform the input data from a dictionary to a list of activities, as it uses the ``locations`` data."""
    location_mapping = {obj["code"]: obj["name"] for obj in data["locations"].values()}

    for act in data["processes"].values():
        for exc in act["exchanges"]:
            if "location" in exc["flow"]:
                exc["flow"]["location"] = location_mapping.get(
                    exc["flow"]["location"], exc["flow"]["location"]
                )

    return data


def json_ld_convert_unit_to_reference_unit(db):
    """Convert the units to their reference unit. Also changes the format to eliminate unnecessary complexity.

    Changes:

        {
            'flow': {'refUnit': 'MJ', ...},
            'unit': {
                '@type': 'Unit',
                '@id': '86ad2244-1f0e-4912-af53-7865283103e4',
                'name': 'kWh'
        }

    To:

        {
            'flow': {...},
            'unit': 'MJ'
        }


    """
    unit_conversion = {
        unit["@id"]: unit["conversionFactor"]
        for group in db["unit_groups"].values()
        for unit in group["units"]
    }

    for ds in db["processes"].values():
        for exc in ds["exchanges"]:
            unit_obj = exc.pop("unit")
            exc["amount"] *= unit_conversion[unit_obj["@id"]]
            exc["unit"] = exc["flow"].pop("refUnit")
    return db


def json_ld_get_normalized_exchange_units(data):
    """The exchanges unit strings are not necessarily the same as BW units. Fix this inconsistency."""
    for act in data:
        for exc in act["exchanges"]:
            if "unit" in exc:
                exc["unit"] = normalize_units_function(exc["unit"])
    return data


def json_ld_add_activity_unit(db):
    """Add units to activities from their reference products."""
    for ds in db:
        potential_units = []
        for exc in ds["exchanges"]:
            if exc.get("quantitativeReference", False):
                potential_units.append(exc["unit"]["name"])
        assert len(potential_units) <= 1
        if len(potential_units) == 0:
            ds["unit"] = None
        elif len(potential_units) == 1:
            ds["unit"] = potential_units[0]
        print(ds["unit"])
    return db


def json_ld_get_activities_list_from_rawdata(data):
    """Return list of processes from raw data."""
    return list(data["processes"].values())


def json_ld_rename_metadata_fields(db):
    """Change metadata field names from the JSON-LD `processes` to BW schema.

    BW schema: https://wurst.readthedocs.io/#internal-data-format

    """

    fields_new_old = [
        ("@id", "code"),
        ("category", "classifications"),
    ]

    for ds in db:
        for given, desired in fields_new_old:
            try:
                ds[desired] = ds.pop(given)
            except KeyError:
                pass

    return db


def json_ld_label_exchange_type(db):
    for act in db:
        for exc in act["exchanges"]:
            if exc.get("flow", {}).get("flowType") == "ELEMENTARY_FLOW":
                exc["type"] = "biosphere"
            elif exc.get("avoidedProduct"):
                if exc.get("input"):
                    raise ValueError("Avoided products are outputs, not inputs")
                exc["type"] = "substitution"
            elif exc["input"]:
                if not exc.get("flow", {}).get("flowType") == "PRODUCT_FLOW":
                    raise ValueError("Inputs must be products")
                exc["type"] = "technosphere"
            else:
                if not exc.get("flow", {}).get("flowType") == "PRODUCT_FLOW":
                    raise ValueError("Outputs must be products")
                exc["type"] = "production"
            # TBD: flowType WASTE_FLOW (Output or input?)

    return db
