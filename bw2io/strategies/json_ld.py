def json_ld_convert_db_dict_into_list(db_dict):
    """Convert dictionary of processes into list of processes."""
    for key, value in db_dict.items():
        assert key == value["@id"]
    return list(db_dict['processes'].values())


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


def json_ld_get_normalized_exchange_locations(db):
    """The exchanges location strings are not necessarily the same as those given in the process or the master metadata. Fix this inconsistency."""
    location_mapping = {obj['code']: obj['name'] for obj in db['locations'].values()}

    for act in db['processes'].values():
        for exc in act['exchanges']:
            if 'location' in exc['flow']:
                exc['flow']['location'] = location_mapping.get(exc['flow']['location'], exc['flow']['location'])

    return db
