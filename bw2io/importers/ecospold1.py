from bw2data import Database
from .base_lci import LCIImporter
from ..extractors import Ecospold1DataExtractor
from ..strategies import (
    assign_only_product_as_production,
    clean_integer_codes,
    drop_unspecified_subcategories,
    es1_allocate_multioutput,
    link_external_technosphere_by_activity_hash,
    link_internal_technosphere_by_activity_hash,
    link_iterable_by_fields,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    set_code_by_activity_hash,
    strip_biosphere_exc_locations,
)
from bw2data import config
from time import time
import functools


class SingleOutputEcospold1Importer(LCIImporter):
    """The default strategy will already set the single product as reference product, name, etc.

    Applies the following strategies:
    #. If only one exchange is a production exchange, that is the reference product
    #. Drop ``unspecified`` subcategories from biosphere flows
    #. Normalize biosphere flow categories to ecoinvent 3.1 standard
    #. Normalize biosphere flow names to ecoinvent 3.1 standard
    #. Create a ``code`` from the activity hash of the dataset
    #. Link to the default biosphere database by name, unit, categories
    #. Link internal technosphere exchanges by name, unit, location, categories
    #. Mark unlinked exchanges

    """
    strategies = [
        assign_only_product_as_production,
        drop_unspecified_subcategories,
        normalize_biosphere_categories,
        normalize_biosphere_names,
        strip_biosphere_exc_locations,
        set_code_by_activity_hash,
        functools.partial(link_iterable_by_fields,
            other=Database(config.biosphere),
            kind='biosphere'
        ),
        link_internal_technosphere_by_activity_hash,
    ]
    format = u"Ecospold1"

    def __init__(self, filepath, db_name):
        self.db_name = db_name
        start = time()
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))

    def match_background(self, background_db):
        """Link exchanges to background database named ``background_db`` using activity hashes"""
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
        drop_unspecified_subcategories,
        clean_integer_codes,
        assign_only_product_as_production,
        strip_biosphere_exc_locations,
        set_code_by_activity_hash,
        functools.partial(link_iterable_by_fields,
            other=Database(config.biosphere),
            kind='biosphere'
        ),
        link_internal_technosphere_by_activity_hash,
    ]

