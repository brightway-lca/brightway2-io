import json
from pathlib import Path


FILES_TO_IGNORE = {
    "context.json",
    "layout.json",
}

DIRECTORIES_TO_IGNORE = {
    "bin",
}

IGNORE_ME = lambda x: x.startswith(".")


def walk_dir(dirpath):
    return [
        (os.path.splitext(file)[0], os.path.join(dirpath, file))
        for file in os.listdir(dirpath)
        if os.path.isfile(os.path.join(dirpath, file)) and is_json_file(file)
    ]


def is_json_file(filepath):
    return str(filepath).lower().endswith("json")


class JSONLDExtractor(object):
    @classmethod
    def extract(cls, filepath):
        filepath = Path(filepath)
        if filepath.is_file():
            if not filepath.suffix == ".zip":
                raise ValueError(
                    "File not supported:\n\t`%s` is a file but not a zip archive."
                )
            else:
                raise NotImplementedError(
                    "Extraction of zip archives not yet supported"
                )
        else:
            assert filepath.is_dir()
            filepath = filepath.resolve()

            # Assume directory is one level deep
            data = {
                directory.name: dict(
                    sorted(
                        [
                            (fp.stem, json.load(open(fp)))
                            for fp in directory.iterdir()
                            if fp.name not in FILES_TO_IGNORE
                            and not fp.name.startswith(".")
                        ]
                    )
                )
                for directory in filepath.iterdir()
                if directory.is_dir() and directory.name not in DIRECTORIES_TO_IGNORE
            }

        return data
