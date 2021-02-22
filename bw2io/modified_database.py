from __future__ import print_function
from . import activity_hash
from bw2data import databases, Database
import collections
import copy


class ModifiedDatabase(object):
    """Find relationships between foreground data ``data`` and background database named ``ref_database_name``.

    Each activity and exchange is summarized in a *hash*, a small set of letters that summarizes all relevant attributes.

    foreground_activities_mapping:
        hash: dataset

    foreground_exchanges_mapping:
        hash: exchange

    foreground_activities:
        activity hash: set of (exchange hash, amount) exchange tuples.

    background_activities_mapping:
        hash: Activity

    background_exchanges_mapping:
        hash: Exchange

    background_activities:
        activity hash: set of (Exchange hash, amount) exchange tuples"""

    def __init__(self, data, ref_database_name, from_simapro=False):
        self.data = data
        self.assert_data_fully_linked()
        self.fields = ("name", "location", "unit") if from_simapro else None
        assert ref_database_name in databases, u"Invalid reference database name"
        self.ref_database = Database(ref_database_name)

    def assert_data_fully_linked(self):
        for ds in self.data:
            for exc in ds.get("exchanges", []):
                if "input" not in exc:
                    raise AssertionError("Database not full linked")

    def iterate_unmatched(self):
        """Return data on activities in ``data`` which can't be found in ``ref_database_name``."""
        for key, value in self.foreground_activities.items():
            if key not in self.background_activities:
                yield (key, value)

    def get_reason(self, exc_tuple, data):
        """Get reason why exc_tuple not in data. Reasons are:
            1) Changed amount
            2) Missing
        """
        if exc_tuple[0] not in [obj[0] for obj in data]:
            return "Missing"
        else:
            matched_amounts = ", ".join(
                ["{}".format(obj[1]) for obj in data if obj[0] == exc_tuple[0]]
            )
            return "New amount: {} to {}".format(exc_tuple[1], matched_amounts)

    def iterate_modified(self):
        """Return data on modified activities"""
        for key, activity in self.foreground_activities.items():
            if key not in self.background_activities:
                continue
            bg = self.background_activities[key]
            fg = self.foreground_activities[key]
            if fg != bg:
                yield (
                    key,
                    {
                        k: v
                        for k, v in self.foreground_activities_mapping[key].items()
                        if k != "exchanges"
                    },
                    [
                        {
                            "reason": self.get_reason(obj, bg),
                            "exchange": self.foreground_exchanges_mapping[obj[0]],
                        }
                        for obj in fg.difference(bg)
                    ],
                    [
                        {
                            "reason": self.get_reason(obj, fg),
                            "exchange": self.background_exchanges_mapping[obj[0]],
                        }
                        for obj in bg.difference(fg)
                    ],
                )

    def load_datasets(self):
        """Determine which datasets are modified by comparing the exchanges values.

        Specifically, compare the set of ``(input activity hashes, amount_as_string)`` values.

        If the name or other important attributes changed, then there won't be a correspondence at all, so the dataset is treated as modified in any case."""
        print(u"Loading foreground data")
        self.foreground_activities_mapping = {
            activity_hash(obj, fields=self.fields): obj for obj in self.data
        }
        self.foreground_exchanges_mapping = {
            activity_hash(exc, fields=self.fields): exc
            for obj in self.data
            for exc in obj.get("exchanges", [])
        }
        self.foreground_activities = {
            key: self.hash_foreground_exchanges(value)
            for key, value in self.foreground_activities_mapping.items()
        }

        print(u"Loading background activities")
        self.background_activities_mapping = {
            activity_hash(obj, fields=self.fields): obj for obj in self.ref_database
        }
        self.background_exchanges_mapping = {}
        print(u"Loading background exchanges")
        self.background_activities = {
            key: self.hash_background_exchanges(value)
            for key, value in self.background_activities_mapping.items()
        }

    def add_to_background_exchanges_mapping(self, exc):
        hashed = activity_hash(exc.input, fields=self.fields)
        self.background_exchanges_mapping[hashed] = exc
        return hashed

    def hash_background_exchanges(self, activity):
        return {
            (self.add_to_background_exchanges_mapping(exc), "{:.6G}".format(exc.amount))
            for exc in activity.exchanges()
        }

    def hash_foreground_exchanges(self, activity):
        return {
            (activity_hash(exc, fields=self.fields), "{:.6G}".format(exc["amount"]))
            for exc in activity.get("exchanges", [])
        }

    def prune(self):
        self.modified = self.find_modified_datasets()
        self.keep = copy.deepcopy(self.modified)
        self.ref_product_consumers = collections.defaultdict(list)
        for ds in self.data:
            for exc in (
                obj
                for obj in ds.get("exchanges", [])
                if obj.get("type") == "technosphere"
            ):
                self.ref_product_consumers[
                    activity_hash(exc, fields=self.fields)
                ].append(activity_hash(ds, fields=self.fields))
        # Iteratively add processes that refer to changed reference products.
        # Stop when no new processes are added
        # This assumes that the hash of the reference product is the same as the hash of the activity, i.e.
        # it will break on ecoinvent >= 3.
        raise ValueError
        while True:
            print("Iteration")
            to_add = set()
            for flow in self.keep:
                for consumer in self.ref_product_consumers[flow]:
                    if consumer not in self.keep:
                        to_add.add(consumer)
            if not to_add:
                break
            self.keep = self.keep.union(to_add)
        raise ValueError
