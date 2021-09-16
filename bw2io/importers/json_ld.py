from ..extractors.json_ld import JSONLDExtractor
from ..strategies import (
    add_database_name,
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
        self.strategies = []
        self.data = self.extractor.extract(dirpath)
