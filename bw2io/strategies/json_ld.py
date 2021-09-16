def convert_db_dict_into_list(db_dict):
    """Convert dictionary of processes into list of processes."""
    db_list = []
    for key in db_dict.keys():
        assert key == db_dict[key]["@id"]
        db_list.append(db_dict[key])
    return db_list

def rename_metadata_field_names(db):
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
