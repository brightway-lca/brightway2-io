from .base import ImportBase
from ..extractors import Ecospold1DataExtractor
from ..strategies import (
    assign_only_product_as_production,
    clean_integer_codes,
    es1_allocate_multioutput,
    link_biosphere_by_activity_hash,
    link_external_technosphere_by_activity_hash,
    link_internal_technosphere_by_activity_hash,
    mark_unlinked_exchanges,
    set_code_by_activity_hash,
)
from bw2data import config
from time import time
import functools


class SingleOutputEcospold1Importer(ImportBase):
    """The default strategy will already set the single product as reference product, name, etc."""
    strategies = [
        assign_only_product_as_production,
        set_code_by_activity_hash,
        functools.partial(link_biosphere_by_activity_hash,
                          biosphere_db_name=config.biosphere),
        link_internal_technosphere_by_activity_hash,
        mark_unlinked_exchanges,
    ]
    format = u"Ecospold1"

    def __init__(self, filepath, db_name):
        self.db_name = db_name
        start = time()
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))

    def match_background(self, background_db):
        """Link exchanges to a background database using activity hashes"""
        func_list = [
            functools.partial(
                link_external_technosphere_by_activity_hash,
                external_db_name=background_db
            )
        ]
        self.apply_strategies(func_list)


class MultiOutputEcospold1Importer(SingleOutputEcospold1Importer):
    strategies = [
        es1_allocate_multioutput,
        clean_integer_codes,
        assign_only_product_as_production,
        set_code_by_activity_hash,
        functools.partial(link_biosphere_by_activity_hash,
                          biosphere_db_name=config.biosphere),
        link_internal_technosphere_by_activity_hash,
        mark_unlinked_exchanges,
    ]

