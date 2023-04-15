from ..units import normalize_units as normalize_units_function


def json_ld_get_normalized_exchange_locations(data):
    """
    Normalize exchange location strings to match those given in the process or the master metadata.
    The function takes a dictionary ``data`` as input and replaces exchange location strings with their corresponding names 
    if they do not match the names given in the process or the master metadata. Uses the 'locations' data 
    to create a mapping between location codes and location names.

    Parameters
    ----------
    data : dict
        A dictionary containing information about processes, exchanges, and locations. 

    Returns
    -------
    dict
        A dictionary containing normalized location strings in the exchanges.

    Examples
    --------
    >>> data = {"locations": {"L1": {"code": "L1", "name": "Location 1"}}, 
                "processes": {"P1": {"exchanges": [{"flow": {"location": "L1"}}]}}}
    >>> json_ld_get_normalized_exchange_locations(data)
    {'locations': {'L1': {'code': 'L1', 'name': 'Location 1'}},
    'processes': {'P1': {'exchanges': [{'flow': {'location': 'Location 1'}}]}}}
    """
    location_mapping = {obj["code"]: obj["name"] for obj in data["locations"].values()}

    for act in data["processes"].values():
        for exc in act["exchanges"]:
            if "location" in exc["flow"]:
                exc["flow"]["location"] = location_mapping.get(
                    exc["flow"]["location"], exc["flow"]["location"]
                )

    return data


def json_ld_add_products_as_activities(db, products):
    """"
    Add products as activities to the given database.
    Takes a database and a list of products, and adds the products to the database as activities. 
    The products are added to the end of the database, after the existing activities.

    Parameters
    ----------
    db : list
        A list of activities representing a database.
    products : list
        A list of products to be added to the database as activities.

    Returns
    -------
    list
        A list of activities representing the updated database.

    Examples
    --------
    >>> db = [{'name': 'Activity 1'}, {'name': 'Activity 2'}]
    >>> products = [{'name': 'Product 1'}, {'name': 'Product 2'}]
    >>> json_ld_add_products_as_activities(db, products)
    [{'name': 'Activity 1'}, {'name': 'Activity 2'}, {'name': 'Product 1'}, {'name': 'Product 2'}]
    """
    db.extend(products)
    return db


def json_ld_convert_unit_to_reference_unit(db):
    """
    Convert the units in the given database to their reference units and simplify the format.

    Takes a database represented as a dictionary and converts the units in the exchanges to their reference 
    units. It also simplifies the format of the exchanges to eliminate unnecessary complexity.

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

    Parameters
    ----------
    db : dict
        A dictionary representing a database containing information about processes and exchanges.

    Returns
    -------
    dict
        A dictionary representing the updated database with units converted to reference units and exchanges simplified.

    Examples
    --------
    >>> db = {
                "unit_groups": {
                    "group1": {
                        "id": "group1",
                        "name": "Group 1",
                        "units": [
                            {
                                "@type": "Unit",
                                "@id": "unit1",
                                "name": "kWh",
                                "conversionFactor": 3.6
                            }
                        ]
                    }
                },
                "processes": {
                    "P1": {
                        "exchanges": [
                            {
                                "flow": {"refUnit": "MJ"},
                                "amount": 10.0,
                                "unit": {"@type": "Unit", "@id": "unit1", "name": "kWh"}
                            }
                        ]
                    }
                }
            }
    >>> json_ld_convert_unit_to_reference_unit(db)
    {'unit_groups': {'group1': {'id': 'group1', 'name': 'Group 1', 'units': [{'@type': 'Unit', '@id': 'unit1', 
        'name': 'kWh', 'conversionFactor': 3.6}]}}, 'processes': {'P1': {'exchanges': [{'flow': {'refUnit': 'MJ'}, 
        'amount': 36.0, 'unit': 'MJ'}]}}}
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
            if "refUnit" in exc["flow"]:
                exc["unit"] = exc["flow"].pop("refUnit")
            else:
                exc["unit"] = unit_obj["name"]
    return db


def json_ld_get_normalized_exchange_units(data):
    """
    Normalize the unit strings in the exchanges to match the Brightway units.

    Takes a list of activities represented as a dictionary and normalizes the unit strings in the exchanges 
    to match the Brightway units. Uses a normalization function 'normalize_units_function' to convert 
    non-Brightway units to their corresponding Brightway units.

    Parameters
    ----------
    data : list
        A list of activities represented as dictionaries containing information about processes and exchanges.

    Returns
    -------
    list
        A list of activities represented as dictionaries containing normalized unit strings in the exchanges.

    See Also
    --------
    normalize_units_function : A function used to convert non-Brightway units to their corresponding Brightway units.

    Examples
    --------
    >>> data = [
                {"name": "Activity 1", "exchanges": [{"flow": {"name": "Flow 1"}, "unit": "kg"}]},
                {"name": "Activity 2", "exchanges": [{"flow": {"name": "Flow 2"}, "unit": "tonnes"}]}
            ]
    >>> json_ld_get_normalized_exchange_units(data)
    [{'name': 'Activity 1', 'exchanges': [{'flow': {'name': 'Flow 1'}, 'unit': 'kilogram'}]},
     {'name': 'Activity 2', 'exchanges': [{'flow': {'name': 'Flow 2'}, 'unit': 'ton'}}]]
    """
    for act in data:
        for exc in act["exchanges"]:
            if "unit" in exc:
                exc["unit"] = normalize_units_function(exc["unit"])
    return data


def json_ld_add_activity_unit(db):
    """
    Add units to activities in the given database from their reference products.

    Takes a database represented as a list of dictionaries and adds units to activities in the database based 
    on their reference products. This is done by looking at the production exchanges of each activity and taking the unit of 
    the reference product as the unit of the activity.

    Parameters
    ----------
    db : list
        A list of dictionaries representing a database containing information about processes and exchanges.

    Returns
    -------
    list
        A list of dictionaries representing the updated database with units added to activities.

    Raises
    ------
    AssertionError
        If there is more than one production exchange for an activity.

    Examples
    --------
    >>> db = [
                {"name": "Activity 1", "exchanges": [{"flow": {"name": "Flow 1"}, "unit": "kg"}]},
                {"name": "Activity 2", "exchanges": [{"flow": {"name": "Flow 2"}, "unit": "tonnes"}]}
            ]
    >>> json_ld_add_activity_unit(db)
    [{'name': 'Activity 1', 'exchanges': [{'flow': {'name': 'Flow 1'}, 'unit': 'kg'}, {'flowType': 'PRODUCT_FLOW', 
        'input': False, 'unit': 'kg'}], 'unit': 'kg'},
     {'name': 'Activity 2', 'exchanges': [{'flow': {'name': 'Flow 2'}, 'unit': 'tonnes'}, {'flowType': 'PRODUCT_FLOW', 
        'input': False, 'unit': 'ton'}}]
    """
    for ds in db:
        if ds.get("type") in {"emission", "product"}:
            continue
        production_exchanges = [
            exc
            for exc in ds["exchanges"]
            if exc["flow"]["flowType"] == "PRODUCT_FLOW" and not exc["input"]
        ]
        assert len(production_exchanges) == 1, "Failed allocation"
        ds["unit"] = production_exchanges[0]["unit"]
    return db


def json_ld_get_activities_list_from_rawdata(data):
    """
    Return a list of processes from raw data.

    Takes raw data in the form of a dictionary and returns a list of processes from the 'processes' key of the 
    dictionary.

    Parameters
    ----------
    data : dict
        A dictionary containing raw data.

    Returns
    -------
    list
        A list of dictionaries representing the processes.

    Examples
    --------
    >>> data = {"processes": {"P1": {"name": "Process 1"}, "P2": {"name": "Process 2"}}}
    >>> json_ld_get_activities_list_from_rawdata(data)
    [{'name': 'Process 1'}, {'name': 'Process 2'}]
    """
    return list(data["processes"].values())


def json_ld_rename_metadata_fields(db):
    """
    Change metadata field names in the given database to match the Brightway schema.

    Takes a database represented as a list of dictionaries and changes the metadata field names in the 
    'processes' to match the Brightway schema. This is done by using a mapping between the old and new field names.

    Brightway schema: https://documentation.brightway.dev/en/latest/source/introduction/introduction.html#activity-data-format

    Parameters
    ----------
    db : list
        A list of dictionaries representing a database containing information about processes and exchanges.

    Returns
    -------
    list
        A list of dictionaries representing the updated database with metadata field names changed to match the Brightway 
        schema.

    Examples
    --------
    >>> db = [                {"@id": "P1", "category": "Class 1", "@type": "Process", "lastChange": "2022-02-01"},                {"@id": "P2", "category": "Class 2", "@type": "Process", "lastChange": "2022-03-01"}            ]
    >>> json_ld_rename_metadata_fields(db)
    [{'code': 'P1', 'classifications': 'Class 1', 'type': 'Process', 'modified': '2022-02-01'},     {'code': 'P2', 'classifications': 'Class 2', 'type': 'Process', 'modified': '2022-03-01'}]
    """
    fields_new_old = [
        ("@id", "code"),
        ("category", "classifications"),
        ("@type", "type"),
        ("lastChange", "modified"),
    ]

    for ds in db:
        for given, desired in fields_new_old:
            try:
                ds[desired] = ds.pop(given)
            except KeyError:
                pass

    return db


def json_ld_remove_fields(db):
    """
    Remove specified fields from the given database.

    Takes a database represented as a list of dictionaries and removes specified fields from the dictionary. 
    The fields to be removed are specified in the FIELDS set.

    Parameters
    ----------
    db : list
        A list of dictionaries representing a database containing information about processes and exchanges.

    Returns
    -------
    list
        A list of dictionaries representing the updated database with specified fields removed.

    Examples
    --------
    >>> db = [
            {"name": "Activity 1", "@context": "http://example.com", "processType": "type1", "infrastructureProcess": True}, 
            {"name": "Activity 2", "@context": "http://example.com", "processType": "type2", "infrastructureProcess": False}
        ]
    >>> json_ld_remove_fields(db)
    [{'name': 'Activity 1'}, {'name': 'Activity 2'}]
    """
    FIELDS = {
        "@context",
        "processType",
        "infrastructureProcess",
    }

    for ds in db:
        for field in FIELDS:
            if field in ds:
                del ds[field]
    return db


def json_ld_location_name(db):
    """
    Update location information in the given database.

    Takes a database represented as a list of dictionaries and updates the location information in the 
    'processes' to match the format of the Brightway schema. This is done by taking the name of the location from the 
    'name' key of the location information and replacing the entire location information with just the location name.

    Parameters
    ----------
    db : list
        A list of dictionaries representing a database containing information about processes and exchanges.

    Returns
    -------
    list
        A list of dictionaries representing the updated database with location information in the Brightway schema format.

    Examples
    --------
    >>> db = [
                {"name": "Activity 1", "location": {"name": "Location 1"}},
                {"name": "Activity 2", "location": {"name": "Location 2"}}
            ]
    >>> json_ld_location_name(db)
    [{'name': 'Activity 1', 'location': 'Location 1'}, {'name': 'Activity 2', 'location': 'Location 2'}]
    """
    for ds in db:
        if ds.get("type") in {"emission", "product"}:
            continue
        ds["location"] = ds["location"]["name"]

    return db


def json_ld_fix_process_type(db):
    """
    Fix process type information in the given database.

    Takes a database represented as a list of dictionaries and updates the process type information in the 
    'processes' to match the format of the Brightway schema. This is done by changing the value of the 'type' key from 
    'Process' to 'process'.

    Parameters
    ----------
    db : list
        A list of dictionaries representing a database containing information about processes and exchanges.

    Returns
    -------
    list
        A list of dictionaries representing the updated database with process type information in the Brightway schema 
        format.
        
    Examples
    --------
    >>> db = [
                {"name": "Activity 1", "type": "Process"},
                {"name": "Activity 2", "type": "Product"}
            ]
    >>> json_ld_fix_process_type(db)
    [{'name': 'Activity 1', 'type': 'process'}, {'name': 'Activity 2', 'type': 'Product'}]
    """
    for ds in db:
        if ds["type"] == "Process":
            ds["type"] = "process"
    return db


def json_ld_prepare_exchange_fields_for_linking(db):
    """
    Update exchange information in the given database to prepare for linking.

    Takes a database represented as a list of dictionaries and updates the exchange information in the 
    'processes' to prepare for linking. This is done by deleting unnecessary fields from the exchange dictionary and moving 
    the 'name' and '@id' fields from the 'flow' dictionary to the exchange dictionary as 'name' and 'code' fields.

    Parameters
    ----------
    db : list
        A list of dictionaries representing a database containing information about processes and exchanges.

    Returns
    -------
    list
        A list of dictionaries representing the updated database with exchange information prepared for linking.

    Examples
    --------
    >>> db = [
                {"name": "Activity 1", "exchanges": [
                    {"flow": {"@id": "F1", "name": "Flow 1", "flowType": "PRODUCT_FLOW", "unit": "kg"}, 
                     "amount": 10, "input": False, "type": "technosphere", "uncertainty": {"amount": 0.1}}
                ]},
                {"name": "Activity 2", "exchanges": [
                    {"flow": {"@id": "F2", "name": "Flow 2", "flowType": "PRODUCT_FLOW", "unit": "kg"}, 
                     "amount": 20, "input": True, "type": "biosphere", "uncertainty": {"amount": 0.2}}
                ]}
            ]
    >>> json_ld_prepare_exchange_fields_for_linking(db)
    [{'name': 'Activity 1', 'exchanges': [
        {'amount': 10, 'type': 'technosphere', 'uncertainty': {'amount': 0.1}, 'name': 'Flow 1', 'code': 'F1'}
    ]}, {'name': 'Activity 2', 'exchanges': [
        {'amount': 20, 'type': 'biosphere', 'uncertainty': {'amount': 0.2}, 'name': 'Flow 2', 'code': 'F2'}
    ]}]
    """
    FIELDS_TO_DELETE = {
        "input",
        "internalId",
        "quantitativeReference",
        "avoidedProduct",
        "flowProperty",
        "@type",
    }

    for ds in db:
        for exc in ds["exchanges"]:
            for field in FIELDS_TO_DELETE:
                if field in exc:
                    del exc[field]

            flow = exc.pop("flow")
            exc["name"] = flow["name"]
            exc["code"] = flow["@id"]

    return db


def json_ld_label_exchange_type(db):
    """
    Add exchange type labels to each exchange in a given life cycle inventory represented as a list of activities and their exchanges.

    Parameters:
    -----------
    db : list
        A list of activities and their exchanges in a life cycle inventory.

    Raises:
    -------
    ValueError
        If an avoided product is considered as an input or if an input exchange is not a product, or if an output exchange is not a product or waste flow.

    Returns:
    --------
    list
        A modified list of activities and their exchanges with the addition of exchange type labels.

    Examples:
    ---------
    >>> db = [{'exchanges': [{'flow': {'flowType': 'PRODUCT_FLOW'}}, {'flow': {'flowType': 'ELEMENTARY_FLOW'}}]},
    ...       {'exchanges': [{'avoidedProduct': True}]},
    ...       {'exchanges': [{'input': True, 'flow': {'flowType': 'WASTE_FLOW'}}]},
    ...       {'exchanges': [{'flow': {'flowType': 'WASTE_FLOW'}}]}]
    >>> json_ld_label_exchange_type(db)
    [{'exchanges': [{'flow': {'flowType': 'PRODUCT_FLOW'}, 'type': 'technosphere'}, {'flow': {'flowType': 'ELEMENTARY_FLOW'}, 'type': 'biosphere'}]},
     {'exchanges': [{'avoidedProduct': True, 'type': 'substitution'}]},
     {'exchanges': [{'input': True, 'flow': {'flowType': 'WASTE_FLOW'}, 'type': 'production'}}],
     {'exchanges': [{'flow': {'flowType': 'WASTE_FLOW'}, 'type': 'production'}]}]
    """
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
                if not exc.get("flow", {}).get("flowType") in (
                    "PRODUCT_FLOW",
                    "WASTE_FLOW",
                ):
                    raise ValueError("Outputs must be products")
                exc["type"] = "production"

    return db
