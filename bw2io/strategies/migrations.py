from ..utils import activity_hash, load_json_data_file, rescale_exchange

# {
#     'fields': ('name', 'categories', 'unit', 'type'),
#     'data': [
#         (
#             ('Water', ('air',), 'kilogram', 'biosphere'),
#             {'unit': 'cubic meter', 'multiplier': 0.001}
#         ),
#         (
#             ('Water', ('air', 'non-urban air or from high stacks'), 'kilogram', 'biosphere'),
#             {'unit': 'cubic meter', 'multiplier': 0.001}
#         ),
#         (
#             ('Water', ('air', 'lower stratosphere + upper troposphere'), 'kilogram', 'biosphere'),
#             {'unit': 'cubic meter', 'multiplier': 0.001}
#         ),
#         (
#             ('Water', ('air', 'urban air close to ground'), 'kilogram', 'biosphere'),
#             {'unit': 'cubic meter', 'multiplier': 0.001}
#         ),
#     ]
# }


def migrate_datasets(db, migrations_file):
    pass


def migrate_exchanges(db, migrations_file):
    migration_data = load_json_data_file(migrations_file)

    to_dict = lambda x: dict(zip(migration_data['fields'], x))

    mapping = {activity_hash(to_dict(obj[0]), fields=migration_data['fields']): obj[1]
        for obj in migration_data['data']}

    for ds in db:
        for exc in ds.get('exchanges', []):
            try:
                new_data = mapping[activity_hash(exc,
                    fields=migration_data['fields'])]
                print "Applying migration!"
                for field, value in new_data.items():
                    if field == 'multiplier':
                        rescale_exchange(exc, value)
                    else:
                        exc[field] = value
            except KeyError:
                pass
    return db
