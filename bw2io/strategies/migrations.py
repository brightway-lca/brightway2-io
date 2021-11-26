from ..errors import MissingMigration
from ..migrations import Migration, migrations
from ..utils import activity_hash, rescale_exchange


def migrate_datasets(db, migration):
    if migration not in migrations:
        raise MissingMigration(
            "Migration `{}` is missing; did you run `bw2setup()` in this project? You can also (re-)install core migrations  with `create_core_migrations()`".format(
                migration
            )
        )
    migration_data = Migration(migration).load()

    to_dict = lambda x: dict(zip(migration_data["fields"], x))

    mapping = {
        activity_hash(to_dict(obj[0]), fields=migration_data["fields"]): obj[1]
        for obj in migration_data["data"]
    }

    for ds in db:
        try:
            new_data = mapping[activity_hash(ds, fields=migration_data["fields"])]
        except KeyError:
            # This dataset is not in the list to be migrated
            continue
        for field, value in new_data.items():
            if field == "multiplier":
                # This change should only be done by `migrate_exchanges`
                continue
            else:
                ds[field] = value
    return db


def migrate_exchanges(db, migration):
    if migration not in migrations:
        raise MissingMigration(
            "Migration `{}` is missing; did you run `bw2setup()` in this project? You can also (re-)install core migrations  with `create_core_migrations()`".format(
                migration
            )
        )
    migration_data = Migration(migration).load()

    to_dict = lambda x: dict(zip(migration_data["fields"], x))

    # Create dict of lookup fields to new data. There shouldn't be
    # duplicates for the lookup fields, as they will be overwritten
    # during mapping creation.
    mapping = {
        activity_hash(to_dict(obj[0]), fields=migration_data["fields"]): obj[1]
        for obj in migration_data["data"]
    }

    for ds in db:
        for exc in ds.get("exchanges", []):
            try:
                new_data = mapping[activity_hash(exc, fields=migration_data["fields"])]
            except KeyError:
                # This exchange is not in the list to be migrated
                continue
            for field, value in new_data.items():
                if field == "multiplier":
                    rescale_exchange(exc, value)
                else:
                    exc[field] = value
    return db
