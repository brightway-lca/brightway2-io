# -*- coding: utf-8 -*-
from ..errors import StrategyError
from ..migrations import migrations
from ..utils import activity_hash
from ..strategies import migrate_datasets, migrate_exchanges
from ..unlinked_data import UnlinkedData, unlinked_data
from datetime import datetime
from time import time
import functools
import warnings


class ImportBase(object):
    """Base class for format-specific importers.

    Defines workflow for applying strategies."""

    def __init__(self, *args, **kwargs):
        raise NotImplemented("This class should be subclassed")

    def __iter__(self):
        for ds in self.data:
            yield ds

    def apply_strategy(self, strategy, verbose=True):
        """Apply ``strategy`` transform to ``self.data``.

        Adds strategy name to ``self.applied_strategies``. If ``StrategyError`` is raised, print error message, but don't raise error.

        .. note:: Strategies should not partially modify data before raising ``StrategyError``.

        Args:
            *strategy* (callable)

        Returns:
            Nothing, but modifies ``self.data``, and strategy to ``self.applied_strategies``.

        """
        if not hasattr(self, "applied_strategies"):
            self.applied_strategies = []
        try:
            func_name = strategy.__name__
        except AttributeError:  # Curried function
            func_name = strategy.func.__name__
        if verbose:
            print("Applying strategy: {}".format(func_name))
        try:
            self.data = strategy(self.data)
            self.applied_strategies.append(func_name)
        except StrategyError as err:
            print("Couldn't apply strategy {}:\n\t{}".format(func_name, err))

    def apply_strategies(self, strategies=None, verbose=True):
        """Apply a list of strategies.

        Uses the default list ``self.strategies`` if ``strategies`` is ``None``.

        Args:
            *strategies* (list, optional): List of strategies to apply. Defaults to ``self.strategies``.

        Returns:
            Nothings, but modifies ``self.data``, and adds each strategy to ``self.applied_strategies``.

        """
        start = time()
        func_list = self.strategies if strategies is None else strategies
        total = len(func_list)
        for i, func in enumerate(func_list):
            self.apply_strategy(func, verbose)
            if hasattr(self, "signal") and hasattr(self.signal, "emit"):
                self.signal.emit(i + 1, total)
        if verbose:
            print(
                "Applied {} strategies in {:.2f} seconds".format(
                    len(func_list), time() - start
                )
            )

    @property
    def unlinked(self):
        """Iterate through unique unlinked exchanges.

        Uniqueness is determined by ``activity_hash``."""
        seen = set()
        for ds in self.data:
            for exc in ds.get("exchanges", []):
                if not exc.get("input"):
                    ah = activity_hash(exc)
                    if ah in seen:
                        continue
                    else:
                        seen.add(ah)
                        yield exc

    def write_unlinked(self, name):
        """Write all data to an ``UnlikedData`` data store (not a ``Database``!)"""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            udb = UnlinkedData(name + " " + self.__class__.__name__)
        if udb.name not in unlinked_data:
            udb.register()
        unlinked_data[udb.name] = {
            "strategies": getattr(self, "applied_strategies", []),
            "modified": datetime.now().isoformat(),
            "kind": "database",
        }
        unlinked_data.flush()
        udb.write(self.data)
        print("Saved unlinked data: {}".format(udb.name))

    def _migrate_datasets(self, migration_name):
        assert migration_name in migrations, "Can't find migration {}".format(
            migration_name
        )
        self.apply_strategy(
            functools.partial(migrate_datasets, migration=migration_name)
        )

    def _migrate_exchanges(self, migration_name):
        assert migration_name in migrations, "Can't find migration {}".format(
            migration_name
        )
        self.apply_strategy(
            functools.partial(migrate_exchanges, migration=migration_name)
        )
