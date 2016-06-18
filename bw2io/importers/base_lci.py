# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import Database, databases, config
from .base import ImportBase
from ..export.excel import write_lci_matching
from ..errors import StrategyError, NonuniqueCode, WrongDatabase
from ..utils import activity_hash
from ..strategies import (
    assign_only_product_as_production,
    drop_unlinked,
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    link_technosphere_based_on_name_unit_location,
    link_technosphere_by_activity_hash,
    normalize_units,
    strip_biosphere_exc_locations,
)
from ..unlinked_data import UnlinkedData, unlinked_data
from datetime import datetime
import collections
import functools
import warnings


class LCIImporter(ImportBase):
    """Base class for format-specific importers.

    Defines workflow for applying strategies.

    Takes a database name (string) as initialization parameter.

    """
    format = "Generic LCIImporter"

    def __init__(self, db_name):
        self.db_name = db_name
        self.strategies = [
            normalize_units,
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
                       backend=None, **kwargs):
        """
Write data to a ``Database``.

All arguments are optional, and are normally not specified.

Args:
    * *data* (dict, optional): The data to write to the ``Database``. Default is ``self.data``.
    * *name* (str, optional): The name of the ``Database`` to create. Default is ``self.db_name``.
    * *overwrite* (bool, optional): Overwrite the ``Database`` if it currently exists. Default is ``True``.
    * *backend* (string, optional): Storage backend to use when creating ``Database``. Default is the default backend.

Returns:
    ``Database`` instance.

        """
        name = self.db_name if name is None else name
        data = self.data if data is None else data

        if {o['database'] for o in data} != {name}:
            raise WrongDatabase
        if len({o['code'] for o in data}) < len(data):
            raise NonuniqueCode

        data = {(ds['database'], ds['code']): ds for ds in data}

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
                db.register(format=self.format, **kwargs)
        existing.update(data)
        db.write(existing)
        print("Created database: {}".format(db.name))
        return db

    def write_excel(self, only_unlinked=False, only_names=False):
        """Write database information to a spreadsheet.

        If ``only_unlinked``, then only write unlinked exchanges.

        If ``only_names``, then write only activity names, no exchange data.

        Returns the filepath to the spreadsheet file.

        """
        fp = write_lci_matching(self.data, self.db_name, only_unlinked, only_names)
        print(u"Wrote matching file to:\n{}".format(fp))

    def match_database(self, db_name=None, fields=None,
                       ignore_categories=False, relink=False, kind=None):
        """Match current database against itself or another database.

        If ``db_name`` is None, match against current data. Otherwise, ``db_name`` should be the name of an existing ``Database``.

        ``fields`` is a list of fields to use for matching. Field values are case-insensitive, but otherwise must match exactly for a link to be valid. If ``fields`` is ``None``, use the default fields of 'name', 'categories', 'unit', 'reference product', and 'location'.

        If ``ignore_categories``, link based only on name, unit and location. ``ignore_categories`` conflicts with ``fields``.

        If ``relink``, relink exchanges even if a link is already present.

        ``kind`` can be a string or a list of strings. Common values are "technosphere", "biosphere", "production", and "substitution".

        Nothing is returned, but ``self.data`` is changed.

        """
        kwargs = {
            'fields': fields,
            'kind': kind,
            'relink': relink,
        }
        if fields and ignore_categories:
            raise ValueError("Choose between `fields` and `ignore_categories`")
        if ignore_categories:
            kwargs['fields'] = {'name', 'unit', 'location'}
        if db_name:
            if db_name not in databases:
                raise StrategyError("Can't find external database {}".format(
                                    db_name))
            kwargs['other'] = Database(db_name)
        else:
            kwargs['internal'] = True

        self.apply_strategy(
            functools.partial(link_iterable_by_fields, **kwargs)
        )

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
