def add_dummy_processes_and_rename_exchanges(db):
    """
    Add new processes to link to so-called "dummy" processes in the US LCI database.

    This function adds new processes to link to dummy processes found in the US LCI
    database and renames the exchanges accordingly.

    Parameters
    ----------
    db : list
        A list of datasets containing exchanges with dummy processes.

    Returns
    -------
    list
        A modified list of datasets with new processes added and exchanges renamed.

    Examples
    --------
    >>> db = [
            {
                "database": "uslci",
                "exchanges": [
                    {
                        "name": "dummy_Production",
                        "input": ("uslci", "dummy_Production"),
                        "type": "production",
                        "amount": 1
                    }
                ]
            }
        ]
    >>> add_dummy_processes_and_rename_exchanges(db)
    [
        {
            "database": "uslci",
            "exchanges": [
                {
                    "name": "dummy_Production",
                    "input": ("uslci", "Production"),
                    "type": "production",
                    "amount": 1
                }
            ]
        },
        {
            "name": "Production",
            "database": "uslci",
            "code": "Production",
            "categories": ("dummy",),
            "location": "GLO",
            "type": "process",
            "exchanges": [
                {
                    "input": ("uslci", "Production"),
                    "type": "production",
                    "amount": 1
                }
            ]
        }
    ]
    """
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
