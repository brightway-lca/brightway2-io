from __future__ import print_function
from bw2data import Database, databases
from ..utils import activity_hash
from ..strategies import (
    assign_only_product_as_production,
    link_biosphere_by_activity_hash,
    mark_unlinked_exchanges,
)
from ..unlinked_databases import UnlinkedData, unlinked_data
from datetime import datetime
import functools
import warnings


class ImportBase(object):
    """Base class for format-specific importers.

    Defines workflow for applying strategies."""
    final_strategies = [
        mark_unlinked_exchanges,
    ]

    format_strategies = []

    default_strategies = [
        assign_only_product_as_production,
    ]

    def __init__(self, *args, **kwargs):
        raise NotImplemented(u"This class should be subclassed")

    def __iter__(self):
        for ds in self.data:
            yield ds

    def apply_strategies(self):
        self.apply_default_strategies()
        self.apply_format_strategies()
        self.apply_final_strategies()

    def _apply_strategies(self, func_list):
        if not hasattr(self, "applied_strategies"):
            self.applied_strategies = []
        for func in func_list:
            try:
                func_name = func.__name__
            except AttributeError:  # Curried function
                func_name = func.func.__name__
            print(u"Applying strategy: {}".format(func_name))
            self.applied_strategies.append(func_name)
            self.data = func(self.data)

    def apply_format_strategies(self):
        self._apply_strategies(self.format_strategies)

    def apply_final_strategies(self):
        self._apply_strategies(self.final_strategies)

    def apply_default_strategies(self):
        self._apply_strategies(self.default_strategies)

    def statistics(self, print_stats=True):
        num_datasets = len(self.data)
        num_exchanges = sum([len(ds.get('exchanges', [])) for ds in self.data])
        num_unlinked = sum([len([exc for exc in ds.get('exchanges', [])
                           if exc.get("unlinked")]) for ds in self.data])
        if print_stats:
            print(u"{} datasets\n{} exchanges\n{} unlinked exchanges".format(
                  num_datasets, num_exchanges, num_unlinked))
        return num_datasets, num_exchanges, num_unlinked

    @property
    def unlinked(self):
        for ds in self.data:
            for exc in ds.get('exchanges', []):
                if exc.get('unlinked'):
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
        db.process()

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

    def match_database(self, db_name, linking_algorithm):
        # TODO
        other_data = Database(db_name)
        # correspondence = {ds['key']}
        # for ds in self.data:
        #     for exc in ds.get('exchanges', []):
        #         if exc.get('unlinked'):
        #             yield exc
        # raise StopIteration


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
        new_bio.process()

        self._apply_strategies([
            functools.partial(link_biosphere_by_activity_hash,
                              biosphere_db_name=biosphere_name),
            mark_unlinked_exchanges,
        ])
