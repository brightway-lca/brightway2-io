from __future__ import print_function
from bw2data import Database, databases, config
from .base import ImportBase
from ..export.excel import write_lci_matching
from ..errors import StrategyError
from ..utils import activity_hash
from ..strategies import (
    assign_only_product_as_production,
    drop_unspecified_subcategories,
    link_technosphere_by_activity_hash,
    link_technosphere_based_on_name_unit_location,
    link_iterable_by_fields,
    strip_biosphere_exc_locations,
)
from ..unlinked_data import UnlinkedData, unlinked_data
from datetime import datetime
import functools
import warnings


class LCIImporter(ImportBase):
    """Base class for format-specific importers.

    Defines workflow for applying strategies."""
    strategies = [
        drop_unspecified_subcategories,
        assign_only_product_as_production,
        strip_biosphere_exc_locations,
    ]

    def __init__(self, *args, **kwargs):
        raise NotImplemented(u"This class should be subclassed")

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

    def write_excel(self):
        fp = write_lci_matching(self.data, self.db_name)
        print(u"Wrote matching file to:\n{}".format(fp))

    def match_database(self, db_name, from_simapro=False):
        if from_simapro:
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
            dct = {key: value for key, value in exc.items() if key in KEYS}
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
                                  other=bio_data,
                                  relink=True),
            ])

    def add_unlinked_flows_to_biosphere_database(self, biosphere_name=None):
        biosphere_name = biosphere_name or config.biosphere
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
            functools.partial(link_iterable_by_fields,
                other=Database(config.biosphere),
                kind='biosphere'
            ),
        ])
