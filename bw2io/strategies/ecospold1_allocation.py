import copy


def es1_allocate_multioutput(data):
    """This strategy allocates multioutput datasets to new datasets.

    This deletes the multioutput dataset, breaking any existing linking. This shouldn't be a concern, as you shouldn't link to a multioutput dataset in any case.

    """
    activities = []
    for ds in data:
        multi_output = [exc for exc in ds[u"exchanges"] \
            if u"reference" in exc]
        if multi_output:
            for activity in self.allocate_exchanges(ds):
                activities.append(activity)
        else:
            activities.append(ds)
    return activities


def allocate_exchanges(self, ds):
    """
Take a dataset, which has multiple outputs, and return a list of allocated datasets.

The allocation data structure looks like:

    {
        'exchanges': [integer codes for biosphere flows, ...],
        'fraction': out of 100,
        'reference': integer codes
    }

    """
    new_datasets = []
    coproducts = [exc for exc in ds["exchanges"]
                  if exc['type'] == 'production']
    multipliers = {}
    for obj in ds['allocations']:
        if not obj['fraction']:
            continue
        for exc_id in obj['exchanges']:
            multipliers.setdefault(obj['reference'], {})[exc_id] = \
                obj['fraction'] / 100.0
    exchange_dict = {exc['code']: exc for exc in ds['exchanges']
                     if exc['type'] != 'production'}
    for coproduct in coproducts:
        new_ds = copy.deepcopy(ds)
        new_ds['products'] = [coproduct]
        new_ds['exchanges'] = [
            rescale_exchange(exchange_dict[exc_id], scale)
            for exc_id, scale
            in multipliers[coproduct['code']].items()
        ]
        new_ds.append(coproduct)
        new_datasets.append(new_ds)
    return new_datasets


def rescale_exchange(exc, scale):
    exc = copy.deepcopy(exc)
    exc['amount'] *= scale
    return exc
