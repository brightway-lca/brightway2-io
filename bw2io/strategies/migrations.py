from ..migrations import Migration, migrations
from ..utils import activity_hash, load_json_data_file, rescale_exchange


def migrate_datasets(db, migration):
    assert migration in migrations, u"Can't find migration {}".format(migration)
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
        print(f"Migration {migration} not found. Trying to add it...")
        from bw2io import create_core_migrations
        create_core_migrations()
    assert migration in migrations, u"Still can't find migration {}. Please file an issue on https://github.com/brightway-lca/brightway2-io".format(migration)
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
