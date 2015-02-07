from ..strategies import (
    assign_only_product_as_reference_product,
    mark_unlinked_exchanges,
)


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

    def apply_strategies(self):
        self.apply_default_strategies()
        self.apply_format_strategies()
        self.apply_final_strategies()

    def _apply_strategies(self, func_list):
        for func in func_list:
            print(u"Applying strategy: {}".format(func.__name__))
            self.data = func(self.data)

    def apply_format_strategies(self):
        self._apply_strategies(self.format_strategies)

    def apply_final_strategies(self):
        self._apply_strategies(self.final_strategies)

    def apply_default_strategies(self):
        self._apply_strategies(self.default_strategies)

    def statistics(self):
        num_datasets = len(self.data)
        num_exchanges = sum([len(ds.get('exchanges', [])) for ds in self.data])
        num_unlinked = sum([len([exc for exc in ds.get('exchanges', [])
                           if exc.get("unlinked")]) for ds in self.data])
        print(u"{} datasets\n{} exchanges\n{} unlinked exchanges".format(
              num_datasets, num_exchanges, num_unlinked))
        return num_datasets, num_exchanges, num_unlinked

    def iter_unlinked(self):
        for ds in self.data:
            for exc in ds.get('exchanges', []):
                if exc.get('unlinked'):
                    yield exc
        raise StopIteration
