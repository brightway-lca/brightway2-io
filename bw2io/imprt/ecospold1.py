from .base import ImportBase
from ..extractors import Ecospold1DataExtractor
from ..strategies import (
    # es1_allocate_multioutput,
    link_biosphere_by_activity_hash,
    link_internal_technosphere_by_activity_hash,
    set_code_by_activity_hash,
)
from time import time


class SingleOutputEcospold1Importer(ImportBase):
    """The default strategy will already set the single product as reference product, name, etc."""
    format_strategies = [
        set_code_by_activity_hash,
        link_biosphere_by_activity_hash,
        link_internal_technosphere_by_activity_hash,
    ]
    format = u"Ecospold1"

    def __init__(self, filepath, db_name):
        self.db_name = db_name
        start = time()
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))


class MultiOutputEcospold1Importer(ImportBase):
    format_strategies = [
        # es1_allocate_multioutput,
        set_code_by_activity_hash,
        link_biosphere_by_activity_hash,
        link_internal_technosphere_by_activity_hash,
    ]

    def __init__(self, filepath, db_name):
        start = time()
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))
