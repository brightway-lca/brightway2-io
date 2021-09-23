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
from bw2data import Database, config
from functools import partial


class JSONLDImporter(LCIImporter):
    """Importer for the `OLCD JSON-LD data format <https://github.com/GreenDelta/olca-schema>`__.

    See `discussion with linked issues here <https://github.com/brightway-lca/brightway2-io/issues/15>`__.

    """

    format = "OLCA JSON-LD"
    extractor = JSONLDExtractor

    def __init__(self, dirpath, database_name, preferred_allocation=None):
        self.data = self.extractor.extract(dirpath)
        self.biosphere_database = self.flows_as_biosphere_database(
            self.data, database_name
        )
        self.products = self.flows_as_products(self.data)
        self.strategies = [
            partial(json_ld_allocate_datasets, preferred_allocation=None),
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
            partial(link_iterable_by_fields, fields=['code'], kind={'production', 'technosphere'}, internal=True),
            partial(link_iterable_by_fields, other=self.biosphere_database, fields=['code'], kind={'biosphere'}),
            normalize_units,
        ]

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
                "CAS number": obj["cas"],
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
                "location": obj['location']['name'] if 'location' in obj else None,
                "exchanges": [],
                "unit": "",
                "type": "product",
            }
            for obj in data["flows"].values()
            if obj["flowType"] == 'PRODUCT_FLOW'
        ]
