# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..utils import activity_hash, load_json_data_file, rescale_exchange
from ..migrations import Migration, migrations


def migrate_datasets(db, migration):
    assert migration in migrations, u"Can't find migration {}".format(migration)
    migration_data = Migration(migration).load()

    to_dict = lambda x: dict(list(zip(migration_data['fields'], x)))

    mapping = {activity_hash(to_dict(obj[0]), fields=migration_data['fields']): obj[1]
        for obj in migration_data['data']}

    for ds in db:
        try:
            new_data = mapping[activity_hash(ds,
                fields=migration_data['fields'])]
            for field, value in list(new_data.items()):
                if field == 'multiplier':
                    # Only rescale production - this will get
                    # inputs and substitution amounts correct
                    for exc in (obj for obj in ds.get('exchanges', [])
                                if obj.get('type') == 'production'):
                        rescale_exchange(exc, value)
                else:
                    ds[field] = value
        except KeyError:
            pass
    return db


def migrate_exchanges(db, migration):
    assert migration in migrations, u"Can't find migration {}".format(migration)
    migration_data = Migration(migration).load()

    to_dict = lambda x: dict(list(zip(migration_data['fields'], x)))

    mapping = {activity_hash(to_dict(obj[0]), fields=migration_data['fields']): obj[1]
        for obj in migration_data['data']}

    for ds in db:
        for exc in ds.get('exchanges', []):
            try:
                new_data = mapping[activity_hash(exc,
                    fields=migration_data['fields'])]
                for field, value in list(new_data.items()):
                    if field == 'multiplier':
                        rescale_exchange(exc, value)
                    else:
                        exc[field] = value
            except KeyError:
                pass
    return db
