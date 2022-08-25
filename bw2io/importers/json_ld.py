from functools import partial

from bw2data import Database, config

from ..errors import NonuniqueCode
from ..extractors.json_ld import JSONLDExtractor
from ..strategies import (
    add_database_name,
    json_ld_add_activity_unit,
    json_ld_add_products_as_activities,
    json_ld_allocate_datasets,
    json_ld_convert_unit_to_reference_unit,
    json_ld_fix_process_type,
    json_ld_get_activities_list_from_rawdata,
    json_ld_get_normalized_exchange_locations,
    json_ld_get_normalized_exchange_units,
    json_ld_label_exchange_type,
    json_ld_location_name,
    json_ld_prepare_exchange_fields_for_linking,
    json_ld_remove_fields,
    json_ld_rename_metadata_fields,
    link_iterable_by_fields,
    normalize_units,
)
from .base_lci import LCIImporter


class JSONLDImporter(LCIImporter):
    """Importer for the `OLCD JSON-LD data format <https://github.com/GreenDelta/olca-schema>`__.

    See `discussion with linked issues here <https://github.com/brightway-lca/brightway2-io/issues/15>`__.

    """

    format = "OLCA JSON-LD"
    extractor = JSONLDExtractor

    def __init__(self, dirpath, database_name, preferred_allocation=None):
        self.data = self.extractor.extract(dirpath)
        self.db_name = database_name
        self._biosphere_database_warned = False
        self.biosphere_database = self.flows_as_biosphere_database(
            self.data, database_name
        )
        self.products = self.flows_as_products(self.data)
        self.strategies = [
            partial(json_ld_allocate_datasets, preferred_allocation=preferred_allocation),
            json_ld_get_normalized_exchange_locations,
            # Transform uncertainties
            json_ld_convert_unit_to_reference_unit,
            json_ld_get_activities_list_from_rawdata,
            partial(json_ld_add_products_as_activities, products=self.products),
            json_ld_get_normalized_exchange_units,
            json_ld_add_activity_unit,
            json_ld_rename_metadata_fields,
            json_ld_location_name,
            json_ld_remove_fields,
            json_ld_fix_process_type,
            json_ld_label_exchange_type,
            json_ld_prepare_exchange_fields_for_linking,
            partial(add_database_name, name=database_name),
            partial(
                link_iterable_by_fields,
                fields=["code"],
                kind={"production", "technosphere"},
                internal=True,
            ),
            partial(
                link_iterable_by_fields,
                other=self.biosphere_database,
                fields=["code"],
                kind={"biosphere"},
            ),
            normalize_units,
        ]

    def apply_strategies(self, *args, **kwargs):
        no_warning = kwargs.pop("no_warning") if "no_warning" in kwargs else False
        super().apply_strategies(*args, **kwargs)
        if self.biosphere_database and not self._biosphere_database_warned:
            if not no_warning:
                MESSAGE = """\n\tCreated {} biosphere flows in separate database '{}'.\n\tUse either `.merge_biosphere_flows()` or `.write_separate_biosphere_database()` to write these flows."""
                print(
                    MESSAGE.format(
                        len(self.biosphere_database),
                        self.biosphere_database[0]["database"],
                    )
                )
            self._biosphere_database_warned = True

    def merge_biosphere_flows(self):
        """Add flows in ``self.biosphere_database`` to ``self.data``."""
        old_db = self.biosphere_database[0]["database"]
        num_flows = len(self.biosphere_database)

        bio_keys = {(self.db_name, obj["code"]) for obj in self.biosphere_database}
        act_keys = {(self.db_name, obj["code"]) for obj in self.data}
        if bio_keys.intersection(act_keys):
            raise NonuniqueCode(
                "Can't merge biosphere flows to main database due to duplicate codes:\n\t{}\n\nFix these codes or use `.write_separate_biosphere_database()`".format(
                    bio_keys.intersection(act_keys)
                )
            )
        for obj in self.biosphere_database:
            obj["database"] = self.db_name
        for obj in self.data:
            for exc in obj.get("exchanges"):
                if exc.get("input") and exc["input"][0] == old_db:
                    exc["input"] = (self.db_name, exc["input"][1])
        self.data.extend(self.biosphere_database)
        self.biosphere_database = []
        print("Moved {} biosphere flows to `self.data`".format(num_flows))

    def write_separate_biosphere_database(self):
        db_name = self.biosphere_database[0]["database"]
        self.write_database(data=self.biosphere_database, db_name=db_name)

    def flows_as_biosphere_database(self, data, database_name, suffix=" biosphere"):
        def boolcheck(lst):
            return tuple([elem for elem in lst if elem is not None])

        category_mapping = {
            obj["@id"]: boolcheck(
                obj.get("category", {}).get("categoryPath", [])
                + [obj.get("category", {}).get("name")]
                + [obj["name"]]
            )
            for obj in data["categories"].values()
        }

        return [
            {
                "code": obj["@id"],
                "name": obj["name"],
                "categories": category_mapping[obj["category"]["@id"]],
                "CAS number": obj.get("cas"),
                "database": database_name + suffix,
                "exchanges": [],
                "unit": "",
                "type": "emission",
            }
            for obj in data["flows"].values()
            if obj["flowType"] == "ELEMENTARY_FLOW"
        ]

    def flows_as_products(self, data):
        def boolcheck(lst):
            return tuple([elem for elem in lst if elem is not None])

        category_mapping = {
            obj["@id"]: boolcheck(
                obj.get("category", {}).get("categoryPath", [])
                + [obj.get("category", {}).get("name")]
                + [obj["name"]]
            )
            for obj in data["categories"].values()
        }

        return [
            {
                "code": obj["@id"],
                "name": obj["name"],
                "categories": category_mapping[obj["category"]["@id"]],
                "location": obj["location"]["name"] if "location" in obj else None,
                "exchanges": [],
                "unit": "",
                "type": "product",
            }
            for obj in data["flows"].values()
            if obj["flowType"] == "PRODUCT_FLOW"
        ]
