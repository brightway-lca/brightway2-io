# -*- coding: utf-8 -*-


def add_dummy_processes_and_rename_exchanges(db):
    """Add new processes to link to so-called "dummy" processes in the US LCI database."""
    new_processes = set()
    for ds in db:
        for exc in ds.get("exchanges"):
            if exc["name"][:6].lower() in ("dummy_", "dummy,"):
                name = exc["name"][6:].lower().strip()
                new_processes.add(name)
                exc["input"] = (ds["database"], name)

    for name in sorted(new_processes):
        db.append(
            {
                "name": name,
                "database": ds["database"],
                "code": name,
                "categories": ("dummy",),
                "location": "GLO",
                "type": "process",
                "exchanges": [
                    {"input": (ds["database"], name), "type": "production", "amount": 1}
                ],
            }
        )

    return db
