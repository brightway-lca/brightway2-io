import functools
import warnings
from datetime import datetime
from time import time

from ..errors import StrategyError
from ..migrations import migrations
from ..strategies import migrate_datasets, migrate_exchanges
from ..unlinked_data import UnlinkedData, unlinked_data
from ..utils import activity_hash


class ImportBase(object):
    """
    Base class for format-specific importers.
    Defines workflow for applying strategies.

    """
    def __init__(self, *args, **kwargs):
        """
        Initialize the ImportBase object.

        Parameters
        ----------
        *args :
            Variable length argument list.
        **kwargs :
            Arbitrary keyword arguments.

        Raises
        ------
        NotImplemented :
            This class should be subclassed.
        
        """
        raise NotImplemented("This class should be subclassed")

    def __iter__(self):
        """
        Iterate over the data and yield the current data.

        Yields
        ------
        ds :
            The current data being iterated over.
        """
        for ds in self.data:
            yield ds

    def apply_strategy(self, strategy, verbose=True):
        """
        Apply the specified strategy transform to the importer's data.

        This method applies a given strategy to the importer's data and logs the applied strategy's name to
        `self.applied_strategies`. If the strategy raises a `StrategyError`, the error message is printed but
        not raised.

        Parameters
        ----------
        strategy : callable
            The strategy function to apply to the importer's data.
        verbose : bool, optional
            If True, print a message indicating which strategy is being applied. Defaults to True.

        Returns
        -------
        None
            Modifies the importer's data in place.

        Raises
        ------
        None
            If the strategy raises a `StrategyError`, the error message is printed but not raised.

        Notes
        -----
        Strategies should not partially modify data before raising a `StrategyError`.

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
        """
        Apply a list of strategies to the importer's data.

        This method applies a list of given strategies to the importer's data and logs the applied strategies' names to
        `self.applied_strategies`. If no list of strategies is provided, it uses `self.strategies`.

        Parameters
        ----------
        strategies : list, optional
            List of strategies to apply. Defaults to `self.strategies`.
        verbose : bool, optional
            If True, print a message indicating which strategy is being applied. Defaults to True.

        Returns
        -------
        None
            Modifies the importer's data in place.

        Notes
        -----
        The method `apply_strategy` is called to apply each individual strategy to the importer's data. Strategies
        that partially modify data before raising a `StrategyError` should be avoided.

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
        """
        Iterate through unique unlinked exchanges.

        Uniqueness is determined by `activity_hash`.

        Yields
        ------
        exc :
            The unlinked exchange that is currently being iterated over.

        """
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
        """
        Write all data to an `UnlinkedData` data store.

        This method writes all of the importer's data to an `UnlinkedData` data store with the specified `name`. The
        `UnlinkedData` object is created with the importer's class name appended to the `name`. The applied strategies
        are logged to the `unlinked_data` dictionary.

        Parameters
        ----------
        name : str
            The name of the `UnlinkedData` data store to be written.

        """
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
        """
        Apply a migration function to the importer's datasets.

        This method applies a given migration function to the importer's datasets, using `migrate_datasets`. The
        migration function must be specified by name in the `migrations` dictionary.

        Parameters
        ----------
        migration_name : str
            The name of the migration function to apply to the importer's datasets.

        Returns
        -------
        None
            Modifies the importer's data in place.

        Raises
        ------
        AssertionError
            If the specified migration function is not in the `migrations` dictionary.

        """
        assert migration_name in migrations, "Can't find migration {}".format(
            migration_name
        )
        self.apply_strategy(
            functools.partial(migrate_datasets, migration=migration_name)
        )

    def _migrate_exchanges(self, migration_name):
        """
        Apply a migration function to the importer's exchanges.

        This method applies a given migration function to the importer's exchanges, using `migrate_exchanges`. The
        migration function must be specified by name in the `migrations` dictionary.

        Parameters
        ----------
        migration_name : str
            The name of the migration function to apply to the importer's exchanges.

        Returns
        -------
        None
            Modifies the importer's data in place.

        Raises
        ------
        AssertionError
            If the specified migration function is not in the `migrations` dictionary.

        """
        assert migration_name in migrations, "Can't find migration {}".format(
            migration_name
        )
        self.apply_strategy(
            functools.partial(migrate_exchanges, migration=migration_name)
        )
