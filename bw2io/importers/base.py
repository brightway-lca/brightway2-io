from __future__ import print_function
from bw2data import Database, databases
from ..export.excel import write_lci_matching
from ..errors import StrategyError
from ..utils import activity_hash
from ..strategies import (
    assign_only_product_as_production,
    drop_unspecified_subcategories,
    link_biosphere_by_activity_hash,
    link_external_technosphere_by_activity_hash,
)
from ..unlinked_data import UnlinkedData, unlinked_data
from datetime import datetime
import functools
import warnings


class ImportBase(object):
    """Base class for format-specific importers.

    Defines workflow for applying strategies."""
    strategies = [
        drop_unspecified_subcategories,
        assign_only_product_as_production,
    ]

    def __init__(self, *args, **kwargs):
        raise NotImplemented(u"This class should be subclassed")

    def __iter__(self):
        for ds in self.data:
            yield ds

    def apply_strategies(self, strategies=None):
        func_list = self.strategies if strategies is None else strategies
        if not hasattr(self, "applied_strategies"):
            self.applied_strategies = []
        for func in func_list:
            try:
                func_name = func.__name__
            except AttributeError:  # Curried function
                func_name = func.func.__name__
            print(u"Applying strategy: {}".format(func_name))
            try:
                self.data = func(self.data)
                self.applied_strategies.append(func_name)
            except StrategyError as err:
                print(u"Couldn't apply strategy {}:\n\t{}".format(func_name, err))

    def statistics(self, print_stats=True):
        num_datasets = len(self.data)
        num_exchanges = sum([len(ds.get('exchanges', [])) for ds in self.data])
        num_unlinked = len([1
                            for ds in self.data
                            for exc in ds.get('exchanges', [])
                            if not exc.get("input")
                            ])
        if print_stats:
            unique_unlinked = len({activity_hash(exc)
                                   for ds in self.data
                                   for exc in ds.get('exchanges', [])
                                   if not exc.get('input')
                                   })
            print(u"{} datasets\n{} exchanges\n{} unlinked exchanges\n{} unique unlinked exchanges".format(
                  num_datasets, num_exchanges, num_unlinked, unique_unlinked))
        return num_datasets, num_exchanges, num_unlinked

    @property
    def unlinked(self):
        """Iterate through unique unlinked exchanges.

        Uniqueness is determined by ``activity_hash``."""
        seen = set()
        for ds in self.data:
            for exc in ds.get('exchanges', []):
                if not exc.get('input'):
                    ah = activity_hash(exc)
                    if ah in seen:
                        continue
                    else:
                        seen.add(ah)
                        yield exc

    def write_database(self, data=None, name=None, overwrite=True,
                       backend=None):
        name = self.db_name if name is None else name
        if name in databases:
            db = Database(name)
            if overwrite:
                existing = {}
            else:
                existing = db.load(as_dict=True)
        else:
            existing = {}
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                db = Database(name, backend=backend)
                db.register(format=self.format)
        data = self.data if data is None else data
        data = {(ds['database'], ds['code']): ds for ds in data}
        existing.update(**data)
        db.write(existing)

        print("Created database: {}".format(db.name))

    def write_unlinked_database(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            udb = UnlinkedData(self.db_name + " (database)")
        if udb.name not in unlinked_data:
            udb.register()
        unlinked_data[udb.name] = {
            'strategies': getattr(self, 'applied_strategies', []),
            'modified': datetime.now().isoformat(),
            'kind': 'database',
        }
        unlinked_data.flush()
        udb.write(self.data)
        print(u"Saved unlinked database: {}".format(udb.name))

    def write_excel(self):
        fp = write_lci_matching(self.data, self.db_name)
        print(u"Wrote matching file to:\n{}".format(fp))

    def match_database(self, db_name, from_simapro=False):
        # if from_simapro:
        #     self.apply_strategies([functools.partial(
        #         link_simapro_technosphere_by_activity_hash,
        #         external_db_name=db_name)
        #     ])
        # else:
        #     self.apply_strategies([functools.partial(
        #         link_external_technosphere_by_activity_hash,
        #         external_db_name=db_name)
        #     ])
        pass
        # TODO

    def create_new_biosphere(self, biosphere_name, relink=True):
        """Create new biosphere database from biosphere flows in ``self.data``.

        Links all biosphere flows to new bio database if ``relink``."""
        assert biosphere_name not in databases, \
            u"{} biosphere database already exists".format(biosphere_name)

        print(u"Creating new biosphere database: {}".format(biosphere_name))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_bio = Database(biosphere_name, backend='singlefile')
            new_bio.register(
                format=self.format,
                comment="New biosphere created by LCI import"
            )

        KEYS = {'name', 'unit', 'categories'}

        def reformat(exc):
            dct = {key: value for key, value in exc.items() if key in KEYS}
            dct.update(type = 'emission', exchanges = [])
            return dct

        bio_data = [reformat(exc) for ds in self.data
                    for exc in ds.get('exchanges', [])
                    if exc['type'] == 'biosphere']

        bio_data = {(new_bio.name, activity_hash(exc)): exc for exc in bio_data}
        new_bio.write(bio_data)

        if relink:
            self.apply_strategies([
                functools.partial(link_biosphere_by_activity_hash,
                                  biosphere_db_name=biosphere_name,
                                  force=True),
            ])

    def add_unlinked_flows_to_biosphere(self, biosphere_name):
        assert biosphere_name in databases, \
            u"{} biosphere database not found".format(biosphere_name)

        bio = Database(biosphere_name)

        KEYS = {'name', 'unit', 'categories'}

        def reformat(exc):
            dct = {key: value for key, value in exc.items() if key in KEYS}
            dct.update(type = 'emission', exchanges = [])
            return dct

        new_data = [reformat(exc) for ds in self.data
                    for exc in ds.get('exchanges', [])
                    if exc['type'] == 'biosphere'
                    and not exc.get('input')]

        data = bio.load()
        data.update(**{(biosphere_name, activity_hash(exc)): exc
                       for exc in new_data})
        bio.write(data)

        self.apply_strategies([
            functools.partial(link_biosphere_by_activity_hash,
                              biosphere_db_name=biosphere_name,
                              force=True),
        ])
