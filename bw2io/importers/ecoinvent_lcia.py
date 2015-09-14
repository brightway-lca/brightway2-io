from .base_lcia import LCIAImporter
from ..data import convert_lcia_methods_data
from ..strategies import (
    set_biosphere_type,
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    normalize_units,
)
import functools
from bw2data import Database, config


class EcoinventLCIAImporter(LCIAImporter):
    def __init__(self):
        # Needs to be in __init__ because config.biosphere is dynamic
        self.strategies = [
            normalize_units,
            set_biosphere_type,
            drop_unspecified_subcategories,
            functools.partial(link_iterable_by_fields,
                other=Database(config.biosphere),
                fields=('name', 'categories')
            ),
        ]
        self.applied_strategies = []
        self.csv_data, self.cf_data, self.file = convert_lcia_methods_data()
        self.separate_methods()

    def separate_methods(self):
        methods = {obj['method'] for obj in self.cf_data}
        metadata = {obj.pop('name'): obj for obj in self.csv_data}
        self.data = [{
            u'filename': self.file,
            u'name': method,
            u'exchanges': [{
                u'name': cf['name'],
                u'categories': cf['categories'],
                u'amount': cf['amount']}
            for cf in self.cf_data
            if cf['method'] == method]
        } for method in methods]
        for obj in self.data:
            obj.update(metadata.get(obj['name'], {}))
