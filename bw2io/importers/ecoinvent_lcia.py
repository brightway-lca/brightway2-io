from .base_lcia import LCIAImporter
from ..data import convert_lcia_methods_data
from ..strategies import (
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    normalize_units,
    rationalize_method_names,
    set_biosphere_type,
)
from bw2data import Database, config
from numbers import Number
import functools
import warnings


class EcoinventLCIAImporter(LCIAImporter):
    def __init__(self):
        # Needs to define strategies in ``__init__`` because
        # ``config.biosphere`` is dynamic
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
        self.csv_data, self.cf_data, self.units, self.file = convert_lcia_methods_data()
        self.separate_methods()

    def add_rationalize_method_names_strategy(self):
        self.strategies.append(rationalize_method_names)

    def separate_methods(self):
        """Separate the list of CFs into distinct methods"""
        methods = {obj['method'] for obj in self.cf_data}
        metadata = {obj['name']: obj for obj in self.csv_data}

        self.data = {}

        missing = set()

        for line in self.cf_data:
            if line['method'] not in self.units:
                missing.add(line['method'])

        if missing:
            _ = lambda x: sorted([str(y) for y in x])
            warnings.warn("Missing units for following:" +
                             " | ".join(_(missing)))

        for line in self.cf_data:
            assert isinstance(line['amount'], Number)

            if line['method'] not in self.data:
                self.data[line['method']] = {
                    'filename': self.file,
                    'unit': self.units.get(line['method'], ''),
                    'name': line['method'],
                    'description': '',
                    'exchanges': []
                }

            self.data[line['method']]['exchanges'].append({
                'name': line['name'],
                'categories': line['categories'],
                'amount': line['amount']
            })

        self.data = list(self.data.values())

        for obj in self.data:
            obj.update(metadata.get(obj['name'], {}))
