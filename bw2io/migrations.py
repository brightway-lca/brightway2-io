from bw2data import config
from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict, JsonWrapper
from .data import (
    get_biosphere_2_3_category_migration_data,
)
import os


class _Migrations(SerializedDict):
    filename = "migrations.json"


migrations = _Migrations()


class Migration(DataStore):
    metadata = migrations
    _intermediate_dir = config.request_dir("migrations")

    @property
    def description(self):
        return self.metadata[self.name]['description']

    def validate(self, *args, **kwargs):
        return

    def process(self):
        return

    def write(self, data, description):
        """Write migration data. Requires a description."""
        try:
            self.assert_registered()
            migrations[self.name]['description'] = description
        except:
            self.register(description=description)
        filepath = os.path.join(
            config.dir,
            self._intermediate_dir,
            self.filename + u".json"
        )
        JsonWrapper.dump(data, filepath)

    def load(self):
        self.assert_registered()
        filepath = os.path.join(
            config.dir,
            self._intermediate_dir,
            self.filename + u".json"
        )
        return JsonWrapper.load(filepath)


def create_core_migrations():
    """Add pre-defined core migrations data files"""
    Migration("biosphere-2-3-categories").write(
        get_biosphere_2_3_category_migration_data(),
        u"Change biosphere category and subcategory labels to ecoinvent version 3"
    )
