# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import config, projects
from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict, JsonWrapper
from .data import (
    get_biosphere_2_3_category_migration_data,
    get_biosphere_2_3_name_migration_data,
    get_simapro_ecoinvent_3_migration_data,
    get_us_lci_migration_data,
)
from .units import get_default_units_migration_data
import os


class _Migrations(SerializedDict):
    filename = "migrations.json"


migrations = _Migrations()


class Migration(DataStore):
    _metadata = migrations
    _intermediate_dir = projects.request_directory("migrations")

    @property
    def description(self):
        return self.metadata['description']

    def validate(self, *args, **kwargs):
        return

    def write(self, data, description):
        """Write migration data. Requires a description."""
        try:
            self.register()
            migrations[self.name]['description'] = description
        except:
            self.register(description=description)
        filepath = os.path.join(
            projects.dir,
            self._intermediate_dir,
            self.filename + u".json"
        )
        JsonWrapper.dump(data, filepath)

    def load(self):
        self.register()
        filepath = os.path.join(
            projects.dir,
            self._intermediate_dir,
            self.filename + u".json"
        )
        return JsonWrapper.load(filepath)


def create_core_migrations():
    """Add pre-defined core migrations data files"""
    Migration(u"biosphere-2-3-categories").write(
        get_biosphere_2_3_category_migration_data(),
        u"Change biosphere category and subcategory labels to ecoinvent version 3"
    )
    Migration(u"biosphere-2-3-names").write(
        get_biosphere_2_3_name_migration_data(),
        u"Change biosphere flow names to ecoinvent version 3"
    )
    Migration(u"simapro-ecoinvent-3").write(
        get_simapro_ecoinvent_3_migration_data(),
        u"Change SimaPro to ecoinvent 3 names"
    )
    Migration(u"us-lci").write(
        get_us_lci_migration_data(),
        u"Fix names in US LCI database"
    )
    Migration(u"default-units").write(get_default_units_migration_data(),
        u"Convert to default units"
    )
