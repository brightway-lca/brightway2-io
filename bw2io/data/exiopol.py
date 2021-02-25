from bw2data import databases, Database
from stats_arrays import UndefinedUncertainty
import csv
import numpy as np
import os
import pyprind


def import_exiopol_IO_table(database_name, dir_path):
    assert os.path.exists(dir_path) and os.path.isdir(
        dir_path
    ), "Problem with given directory path"
    assert database_name not in databases, "Database {} already exists".format(
        database_name
    )
    assert "mrIot_version2.2.2.txt" in os.listdir(
        dir_path
    ), "Directory path must contain `mrIot_version2.2.2.txt` file."

    print("Loading and processing data")

    fp = os.path.join(dir_path, "mrIot_version2.2.2.txt")
    data = [line for line in csv.reader(open(fp, "r"), delimiter="\t")]
    labels = [tuple(x[:3]) for x in data[2:]]
    labels_dict = {i: obj for i, obj in enumerate(labels)}
    data = np.array([[float(x) for x in row[3:]] for row in data[2:]])

    codify = lambda x: ":".join(x[:2])

    def get_column_tech_exchanges(index, obj):
        excs = []
        for row_i, value in enumerate(data[:, index]):
            if not value:
                continue
            elif row_i == index:
                excs.append(
                    {
                        "type": "production",
                        "uncertainty_type": UndefinedUncertainty.id,
                        "amount": float(1 - value),
                        "loc": float(1 - value),
                        "input": (database_name, obj),
                        "output": (database_name, obj),
                    }
                )
            else:
                excs.append(
                    {
                        "type": "technosphere",
                        "uncertainty_type": UndefinedUncertainty.id,
                        "amount": float(value),
                        "loc": float(value),
                        "input": (database_name, codify(labels_dict[row_i])),
                        "output": (database_name, obj),
                    }
                )
        return excs

    print("Creating LCA datasets")
    db = []
    pbar = pyprind.ProgBar(len(labels))
    for index, ds in enumerate(labels):
        db.append(
            {
                "location": ds[0],
                "name": ds[1],
                "unit": ds[2],
                "exchanges": get_column_tech_exchanges(index, codify(ds)),
                "type": "process",
                "database": database_name,
                "code": codify(ds),
            }
        )
        pbar.update()

    print("Writing datasets")
    db_obj = Database(database_name)
    db_obj.register(directory=dir_path)
    # db_obj.write({(ds['database'], ds['code']): ds for ds in db})
    return db_obj
