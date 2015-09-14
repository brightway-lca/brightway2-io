# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .base import ImportBase
from ..export.excel import write_lcia_matching
from ..strategies import (
    drop_unlinked_cfs,
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    match_subcategories,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    normalize_units,
    set_biosphere_type,
)
from ..unlinked_data import UnlinkedData, unlinked_data
from bw2data import methods, Method, mapping, config, Database, databases
from bw2data.utils import recursive_str_to_unicode
from datetime import datetime
from time import time
import functools
import warnings


class LCIAImporter(ImportBase):
    def __init__(self, filepath, biosphere=None):
        self.applied_strategies = []
        self.filepath = filepath
        self.biosphere_name = biosphere or config.biosphere
        if self.biosphere_name not in databases:
            raise ValueError("Can't find biosphere database {}".format(
                            self.biosphere_name))
        self.strategies = [
            normalize_units,
            set_biosphere_type,
            drop_unspecified_subcategories,
            functools.partial(normalize_biosphere_categories, lcia=True),
            functools.partial(normalize_biosphere_names, lcia=True),
            functools.partial(link_iterable_by_fields,
                              other=(obj for obj in Database(self.biosphere_name)
                                     if obj.get("type") == "emission"),
                              kind="biosphere",
                              ),
            functools.partial(match_subcategories,
                              biosphere_db_name=self.biosphere_name),
        ]

    def write_methods(self, overwrite=False):
        num_methods, num_cfs, num_unlinked = self.statistics(False)
        if num_unlinked:
            raise ValueError((u"Can't write unlinked methods ({} unlinked cfs)"
                              ).format(num_unlinked))
        for ds in self.data:
            if ds['name'] in methods:
                if overwrite:
                    del methods[ds['name']]
                else:
                    raise ValueError((u"Method {} already exists. Use "
                        u"``overwrite=True`` to overwrite existing methods"
                        ).format(ds['name']))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                method = Method(ds['name'])
                method.register(
                    description=ds['description'],
                    filename=ds['filename'],
                    unit=ds['unit'],
                )
                method.write(self._reformat_cfs(ds['exchanges']))
                method.process()
        print(u"Wrote {} LCIA methods with {} characterization factors".format(num_methods, num_cfs))

    def write_excel(self, name):
        fp = write_lcia_matching(self.data, name)
        print(u"Wrote matching file to:\n{}".format(fp))

    def drop_unlinked(self):
        self.apply_strategies([drop_unlinked_cfs])

    def _reformat_cfs(self, ds):
        # Note: This assumes no uncertainty or regionalization
        return [((obj['input']), obj['amount'])
                for obj in ds]

    def _format_flow(self, cf):
        # TODO
        return (self.biosphere_name, cf['code']), {
            'exchanges': [],
            'categories': cf['categories'],
            'name': cf['name'],
            'type': ("resource" if cf["categories"][0] == "resource"
                     else "emission"),
            'unit': cf['unit'],
        }

    def add_missing_cfs(self):
        return
        # TODO
        # Have to think about how to do this; how to generate code
        # Check if code not already in existing biosphere db
        new_flows = []
        for method in self.data:
            for cf in method['exchanges']:
                if (self.biosphere_name, cf['code']) not in mapping:
                    new_flows.append(cf)

        new_flows = recursive_str_to_unicode(dict(
            [self._format_flow(cf) for cf in new_flows]
        ))

        if new_flows:
            biosphere = Database(config.biosphere)
            biosphere_data = biosphere.load()
            biosphere_data.update(new_flows)
            biosphere.write(biosphere_data)

            print(u"Added {} new biosphere flows".format(len(new_flows)))

    def statistics(self, print_stats=True):
        num_methods = len(self.data)
        num_cfs = sum([len(ds['exchanges']) for ds in self.data])
        num_unlinked = sum([len([1 for cf in ds['exchanges'] if not cf.get('input')])
                           for ds in self.data])
        if print_stats:
            print(u"{} methods\n{} cfs\n{} unlinked cfs".format(
                  num_methods, num_cfs, num_unlinked))
        return num_methods, num_cfs, num_unlinked

    def migrate(migration_name):
        self._migrate_exchanges(migration_name)
