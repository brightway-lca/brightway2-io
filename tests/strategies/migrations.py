import pytest
from bw2data.tests import bw2test

from bw2io.errors import MissingMigration
from bw2io.strategies import migrate_datasets, migrate_exchanges


@bw2test
def test_migrate_exchanges_missing_migration():
    with pytest.raises(MissingMigration):
        migrate_exchanges([], "foo")


@bw2test
def test_migrate_datasets_missing_migration():
    with pytest.raises(MissingMigration):
        migrate_datasets([], "foo")
