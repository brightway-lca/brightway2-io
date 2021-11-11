from bw2data import Database, databases

from ..extractors.json_ld import JSONLDExtractor
from ..strategies import (
    json_ld_lcia_add_method_metadata,
    json_ld_lcia_convert_to_list,
    json_ld_lcia_reformat_cfs_as_exchanges,
    json_ld_lcia_set_method_metadata,
    normalize_units,
)
from .base_lcia import LCIAImporter


class JSONLDLCIAImporter(LCIAImporter):
    """Importer for the `OLCD JSON-LD LCIA data format <https://github.com/GreenDelta/olca-schema>`__."""

    format = "OLCA JSON-LD"
    extractor = JSONLDExtractor

    def __init__(self, dirpath):
        self.data = self.extractor.extract(dirpath)
        KEEP = ("lcia_categories", "lcia_methods", "flows")
        for key in list(self.data.keys()):
            if key not in KEEP:
                del self.data[key]

        self.strategies = [
            json_ld_lcia_add_method_metadata,
            json_ld_lcia_convert_to_list,
            json_ld_lcia_set_method_metadata,
            json_ld_lcia_reformat_cfs_as_exchanges,
            normalize_units,
        ]

    def match_biosphere_by_id(self, database_name):
        assert database_name in databases

        codes = {o["code"] for o in Database(database_name)}

        for method in self.data:
            for cf in method["exchanges"]:
                if cf["flow"]["@id"] in codes:
                    cf["input"] = (database_name, cf["flow"]["@id"])
