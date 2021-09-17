from ..extractors.json_ld import JSONLDExtractor
from ..strategies import (
    add_database_name,
    json_ld_add_activity_unit,
    json_ld_convert_db_dict_into_list,
    json_ld_convert_unit_to_reference_unit,
    json_ld_get_activities_list_from_rawdata,
    json_ld_get_normalized_exchange_locations,
    json_ld_get_normalized_exchange_units,
    json_ld_rename_metadata_fields,
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
    normalize_units,
    strip_biosphere_exc_locations,
)
from .base_lci import LCIImporter
from bw2data import Database, config


class JSONLDImporter(LCIImporter):
    """Importer for the `OLCD JSON-LD data format <https://github.com/GreenDelta/olca-schema>`__.

    See `discussion with linked issues here <https://github.com/brightway-lca/brightway2-io/issues/15>`__.

    """

    format = "OLCA JSON-LD"
    extractor = JSONLDExtractor

    def __init__(self, dirpath):
        self.strategies = [
            json_ld_get_normalized_exchange_locations,
            # Transform uncertainties
            json_ld_convert_unit_to_reference_unit,
            json_ld_get_activities_list_from_rawdata,
            json_ld_get_normalized_exchange_units,
            # Allocation
            json_ld_add_activity_unit,
            json_ld_rename_metadata_fields,
            json_ld_convert_db_dict_into_list,
        ]
        self.data = self.extractor.extract(dirpath)
