from __future__ import print_function
from ..export.excel import write_lcia_matching
from ..strategies import (
    add_cf_biosphere_activity_hash,
    drop_unlinked_cfs,
    match_subcategories,
)
from ..unlinked_data import UnlinkedData, unlinked_data
from bw2data import methods, Method, mapping, config, Database
from bw2data.utils import recursive_str_to_unicode
from datetime import datetime
from time import time
import functools
import warnings


class LCIAImportBase(object):
    def __init__(self, filepath, biosphere=None):
        self.applied_strategies = []
        self.filepath = filepath
        self.biosphere_name = biosphere or config.biosphere
        self.strategies = [
            functools.partial(add_cf_biosphere_activity_hash,
                              biosphere_db_name=self.biosphere_name),
            functools.partial(match_subcategories,
                              biosphere_db_name=self.biosphere_name),
        ]

    def __iter__(self):
        for obj in self.data:
            yield obj

    def apply_strategies(self, strategies=None):
        func_list = self.strategies if strategies is None else strategies
        for func in func_list:
            try:
                func_name = func.__name__
            except AttributeError:  # Curried function
                func_name = func.func.__name__
            print(u"Applying strategy: {}".format(func_name))
            self.applied_strategies.append(func_name)
            self.data = func(self.data)

    def write_methods(self, overwrite=False):
        for ds in self.data:
            if ds['name'] in methods:
                if overwrite:
                    del methods[ds['name']]
                else:
                    continue

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
        print(u"Wrote {} LCIA methods".format(len(self.data)))

    def write_excel(self, name):
        fp = write_lcia_matching(self.data, name)
        print(u"Wrote matching file to:\n{}".format(fp))

    def drop_unlinked(self):
        self.apply_strategies([drop_unlinked_cfs])

    def _reformat_cfs(self, ds):
        return [((self.biosphere_name, obj['code']), obj['amount'])
                for obj in ds]

    def _format_flow(self, cf):
        return (config.biosphere, cf['code']), {
            'exchanges': [],
            'categories': cf['categories'],
            'name': cf['name'],
            'type': ("resource" if cf["categories"][0] == "resource"
                     else "emission"),
            'unit': cf['unit'],
        }

    def write_unlinked_methods(self, name, overwrite=False):
        if name in unlinked_data and not overwrite:
            raise ValueError("This unlinked data already exists; call with "
                             "`overwrite=True` to replace.")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            udb = UnlinkedData(name)
        if name not in unlinked_data:
            udb.register()

        unlinked_data[name] = {
            'strategies': getattr(self, 'applied_strategies', []),
            'modified': datetime.now().isoformat(),
            'kind': 'method',
        }
        unlinked_data.flush()
        udb.write(self.data)
        print(u"Saved unlinked methods: {}".format(name))

    def add_missing_cfs(self):
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
            biosphere_data.update(**new_flows)
            biosphere.write(biosphere_data)

            print(u"Added {} new biosphere flows".format(len(new_flows)))

    @property
    def unlinked(self):
        for ds in self.data:
            for exc in ds.get('exchanges', []):
                if not exc.get('code'):
                    yield exc

    def statistics(self, print_stats=True):
        num_methods = len(self.data)
        num_cfs = sum([len(ds['exchanges']) for ds in self.data])
        num_unlinked = sum([len([1 for cf in ds['exchanges'] if not cf.get('code')])
                           for ds in self.data])
        if print_stats:
            print(u"{} methods\n{} cfs\n{} unlinked cfs".format(
                  num_methods, num_cfs, num_unlinked))
        return num_methods, num_cfs, num_unlinked
