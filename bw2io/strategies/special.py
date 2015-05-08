# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals, division
from eight import *


def add_dummy_processes(db):
    """Add new processes to link to so-called "dummy" processes in the US LCI database."""
    new_processes = set()
    for ds in db:
        for exc in ds.get('exchanges'):
            if exc['name'][:6].lower() == "dummy_":
                name = exc['name'][6:].lower()
                new_processes.add(name)
                exc['input'] = (ds['database'], name)

    for name in sorted(new_processes):
        db.append({
            'name': name,
            'database': ds['database'],
            'code': name,
            'categories': ('dummy',),
            'location': 'GLO',
            'type': 'process',
            'exchanges': [{
                'input': (ds['database'], name),
                'type': 'production',
                'amount': 1
            }],
        })

    return db
