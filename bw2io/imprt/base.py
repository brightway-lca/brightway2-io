from bw2data import Database, databases
from ..strategies import (
    assign_only_product_as_reference_product,
    mark_unlinked_exchanges,
)
import warnings


class ImportBase(object):
    """Base class for format-specific importers.

    Defines workflow for applying strategies."""
    final_strategies = [
        mark_unlinked_exchanges,
    ]

    format_strategies = []

    default_strategies = [
        assign_only_product_as_reference_product,
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
        for func in func_list:
            try:
                func_name = func.__name__
            except AttributeError:  # Curried function
                func_name = func.func.__name__
            print(u"Applying strategy: {}".format(func_name))
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

    def write_database(self, data=None, name=None, overwrite=True):
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
                db = Database(name)
                db.register(format=self.format)
        data = self.data if data is None else data
        data = {(ds['database'], ds['code']): ds for ds in data}
        existing.update(**data)
        db.write(existing)
        db.process()

    def match_database(self, db_name, linking_algorithm):
        # TODO
        other_data = Database(db_name)
        # correspondence = {ds['key']}
        # for ds in self.data:
        #     for exc in ds.get('exchanges', []):
        #         if exc.get('unlinked'):
        #             yield exc
        # raise StopIteration

