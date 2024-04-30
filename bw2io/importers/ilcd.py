import functools

from .base_lci import LCIImporter
from ..extractors.ilcd import ILCDExtractor
from ..strategies.ilcd import (
    rename_activity_keys,
    set_activity_unit,
    convert_to_default_units,
    map_to_biosphere3,
    set_default_location,
    set_production_exchange,
    setdb_and_code,
    remove_clutter,
    transform_uncertainty,
    set_connexions_based_on_psm,
    reformat_connexions,
)
from ..strategies.generic import assign_only_product_as_production
from ..strategies.migrations import migrate_exchanges, migrate_datasets
from ..strategies import link_iterable_by_fields


class ILCDImporter(LCIImporter):
    def __init__(self, dirpath, dbname):
        self.db_name = dbname
        self.data = ILCDExtractor._extract(dirpath)
        self.data = setdb_and_code(self.data, dbname)

        self.strategies = [
            rename_activity_keys,
            set_production_exchange,
            convert_to_default_units,
            set_activity_unit,
            assign_only_product_as_production,
            map_to_biosphere3,
            set_default_location,
            transform_uncertainty,
            reformat_connexions,
            set_connexions_based_on_psm ,
            # production exchanges
            functools.partial(
                link_iterable_by_fields,
                **{
                    "kind": "production",
                    "fields": ["database", "code"],
                    "internal": True,
                }
            ),
            # internal technosphere
            functools.partial(
                link_iterable_by_fields,
                **{
                    "kind": "technosphere",
                    "fields": ["exchanges_name", "unit"],
                    "internal": True,
                }
            ),
            remove_clutter,
        ]
