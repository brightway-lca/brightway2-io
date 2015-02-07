from .base import ImportBase
from ..extractors import Ecospold1DataExtractor
from ..strategies import (
    # es1_allocate_multioutput,
    link_biosphere_by_activity_hash,
    link_internal_technosphere_by_activity_hash,
)


class SingleOutputEcospold1Importer(ImportBase):
    format_strategies = [
        link_biosphere_by_activity_hash,
        link_internal_technosphere_by_activity_hash,
    ]

    def __init__(self, filepath, db_name):
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)


class MultiOutputEcospold1Importer(ImportBase):
    format_strategies = [
        # es1_allocate_multioutput,
        link_biosphere_by_activity_hash,
        link_internal_technosphere_by_activity_hash,
    ]

    def __init__(self, filepath, db_name):
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
