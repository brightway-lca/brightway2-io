# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import Database, databases, config
from .base import ImportBase
from ..export.excel import write_lci_matching
from ..errors import StrategyError
from ..utils import activity_hash
from ..strategies import (
    assign_only_product_as_production,
    drop_unlinked,
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    link_technosphere_based_on_name_unit_location,
    link_technosphere_by_activity_hash,
    strip_biosphere_exc_locations,
)
from ..unlinked_data import UnlinkedData, unlinked_data
from datetime import datetime
import collections
import functools
import warnings


class LCIImporter(ImportBase):
    """Base class for format-specific importers.

    Defines workflow for applying strategies."""

    def __init__(self, *args, **kwargs):
        self.strategies = [
            drop_unspecified_subcategories,
            assign_only_product_as_production,
            strip_biosphere_exc_locations,
        ]

    def statistics(self, print_stats=True):
        num_datasets = len(self.data)
        num_exchanges = sum([len(ds.get('exchanges', [])) for ds in self.data])
        num_unlinked = len([1
                            for ds in self.data
                            for exc in ds.get('exchanges', [])
                            if not exc.get("input")
                            ])
        if print_stats:
            unique_unlinked = collections.defaultdict(set)
            for ds in self.data:
                for exc in (e for e in ds.get('exchanges', [])
                            if not e.get('input')):
                    unique_unlinked[exc.get('type')].add(activity_hash(exc))
            unique_unlinked = sorted([(k, len(v)) for k, v
                                      in list(unique_unlinked.items())])

            print((u"{} datasets\n{} exchanges\n{} unlinked exchanges\n  " +
                "\n  ".join([u"Type {}: {} unique unlinked exchanges".format(*o)
                             for o in unique_unlinked])
                ).format(num_datasets, num_exchanges, num_unlinked))
        return num_datasets, num_exchanges, num_unlinked

    def write_database(self, data=None, name=None, overwrite=True,
                       backend=None):
        name = self.db_name if name is None else name
        if name in databases:
            # TODO: Need to update name of database - maybe not worth it?
            # TODO: Raise error if unlinked exchanges?
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
        existing.update(data)
        db.write(existing)
        print("Created database: {}".format(db.name))
        return db

    def write_excel(self, only_unlinked=False):
        fp = write_lci_matching(self.data, self.db_name, only_unlinked)
        print(u"Wrote matching file to:\n{}".format(fp))

    def match_database(self, db_name, ignore_categories=False):
        if ignore_categories:
            self.apply_strategies([functools.partial(
                link_technosphere_based_on_name_unit_location,
                external_db_name=db_name)
            ])
        else:
            self.apply_strategies([functools.partial(
                link_technosphere_by_activity_hash,
                external_db_name=db_name)
            ])

    def create_new_biosphere(self, biosphere_name, relink=True):
        """Create new biosphere database from biosphere flows in ``self.data``.

        Links all biosphere flows to new bio database if ``relink``."""
        assert biosphere_name not in databases, \
            u"{} database already exists".format(biosphere_name)

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
            dct = {key: value for key, value in list(exc.items()) if key in KEYS}
            dct.update(
                type = 'emission',
                exchanges = [],
                database = biosphere_name,
                code = activity_hash(dct)
            )
            return dct

        bio_data = [reformat(exc) for ds in self.data
                    for exc in ds.get('exchanges', [])
                    if exc['type'] == 'biosphere']

        bio_data = {(ds['database'], ds['code']): ds
                     for ds in bio_data}
        new_bio.write(bio_data)

        if relink:
            self.apply_strategies([
                functools.partial(link_iterable_by_fields,
                                  other=list(bio_data.values()),
                                  relink=True),
            ])

    def add_unlinked_flows_to_biosphere_database(self, biosphere_name=None):
        biosphere_name = biosphere_name or config.biosphere
        assert biosphere_name in databases, \
            u"{} biosphere database not found".format(biosphere_name)

        bio = Database(biosphere_name)

        KEYS = {'name', 'unit', 'categories'}

        def reformat(exc):
            dct = {key: value for key, value in list(exc.items()) if key in KEYS}
            dct.update(
                type = 'emission',
                exchanges = [],
                code = activity_hash(dct),
                database = biosphere_name
            )
            return dct

        new_data = [reformat(exc) for ds in self.data
                    for exc in ds.get('exchanges', [])
                    if exc['type'] == 'biosphere'
                    and not exc.get('input')]

        data = bio.load()
        # Dictionary eliminate duplicates
        data.update({(biosphere_name, activity_hash(exc)): exc
                       for exc in new_data})
        bio.write(data)

        self.apply_strategy(
            functools.partial(link_iterable_by_fields,
                other=(obj for obj in Database(biosphere_name)
                       if obj.get('type') == 'emission'),
                kind='biosphere'
            ),
        )

    def migrate(self, migration_name):
        self._migrate_datasets(migration_name)
        self._migrate_exchanges(migration_name)

    def drop_unlinked(self, i_am_reckless=False):
        if not i_am_reckless:
            warnings.warn("This is the nuclear weapon of linking, and should only be used in extreme cases. Must be called with the keyword argument ``i_am_reckless=True``!")
        else:
            self.apply_strategies([drop_unlinked])

    def add_unlinked_activities(self):
        """Add technosphere flows to ``self.data``."""
        if not hasattr(self, "db_name"):
            raise AttributeError(u"Must have valid ``db_name`` attribute")
        ACTIVITY_KEYS = {'location', 'comment', 'name', 'unit', 'categories'}
        new_activities = [{k: v
                    for k, v in list(obj.items())
                    if obj.get('type') == 'technosphere'
                    and k in ACTIVITY_KEYS
        } for obj in self.unlinked]
        for act in new_activities:
            act[u"type"] = u"process"
            act[u"code"] = activity_hash(act)
            act[u"database"] = self.db_name
        self.data.extend(new_activities)
        self.apply_strategy(
            functools.partial(link_iterable_by_fields, other=self.data)
        )
