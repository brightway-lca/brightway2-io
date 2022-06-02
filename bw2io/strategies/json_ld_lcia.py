def json_ld_lcia_add_method_metadata(data):
    for key, value in data["lcia_methods"].items():
        for category in value["impactCategories"]:
            obj = data["lcia_categories"][category["@id"]]
            obj["parent"] = {
                k: value[k] for k in ("name", "description", "version", "lastChange")
            }
    return data


def json_ld_lcia_set_method_metadata(data):
    TO_DELETE = ("@context", "@type")
    for method in data:
        for field in TO_DELETE:
            if field in method:
                del method[field]
        if "referenceUnitName" in method:
            method["unit"] = method.pop("referenceUnitName")
        else:
            method["unit"] = ""
        if "id" not in method:
            method["id"] = method.pop("@id")
        if not isinstance(method['name'], tuple):
            method["name"] = (method["parent"]["name"], method["name"])
        if "\n" not in method.get("description", ""):
            method["description"] = (
                method.get("description", "") + "\n" + method["parent"].get("description")
            )
    return data


def json_ld_lcia_convert_to_list(data):
    return data["lcia_categories"].values()


def json_ld_lcia_reformat_cfs_as_exchanges(data):
    for method in data:
        method["exchanges"] = method.pop("impactFactors")
        for exc in method["exchanges"]:
            exc["amount"] = exc.pop("value")
            exc["unit"] = exc["unit"]["name"]
    return data
