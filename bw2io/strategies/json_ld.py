from ..units import normalize_units as normalize_units_function


def json_ld_get_normalized_exchange_locations(data):
    """The exchanges location strings are not necessarily the same as those given in the process or the master metadata. Fix this inconsistency."""
    location_mapping = {obj['code']: obj['name'] for obj in data['locations'].values()}

    for act in data['processes'].values():
        for exc in act['exchanges']:
            if 'location' in exc['flow']:
                exc['flow']['location'] = location_mapping.get(exc['flow']['location'], exc['flow']['location'])

    return data


def json_ld_get_normalized_exchange_units(data):
    "The exchanges unit strings are not necessarily the same as BW units. Fix this inconsistency."
    for act in data['processes'].values():
        for exc in act['exchanges']:
            if 'refUnit' in exc['flow']:
                exc['flow']['refUnit'] = normalize_units_function(exc['flow']['refUnit'])
            if 'unit' in exc:
                exc['unit']['name'] = normalize_units_function(exc['unit']['name'])
    return data


def json_ld_add_activity_unit(db):
    """Add units to activities from their reference products."""
    for ds in db:
        potential_units = []
        for exc in ds['exchanges']:
            if exc.get('quantitativeReference', False):
                potential_units.append(exc['unit']['name'])
        assert len(potential_units) <= 1
        if len(potential_units)==0:
            ds['unit'] = None
        elif len(potential_units)==1:
            ds['unit'] = potential_units[0]
        print(ds['unit'])
    return db


def json_ld_get_activities_list_from_rawdata(data):
    """Return list of processes from raw data."""
    return list(data['processes'].values())


def json_ld_rename_metadata_fields(db):
    """Change metadata field names from the JSON-LD `processes` to BW schema.

    BW schema: https://wurst.readthedocs.io/#internal-data-format

    """

    fields_new_old = [
        {
            "new_key": "code",
            "old_key": "@id",
        },
        {
            "new_key": "classifications",
            "old_key": "category",
        },
    ]

    for ds in db:
        for field in fields_new_old:
            try:
                ds[field['new_key']] = ds.pop(field['old_key'])
            except:
                pass

    return db
