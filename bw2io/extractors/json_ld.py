# -*- coding: utf-8 -*-
import json
import os


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
        if os.path.isfile
    ]


class JSONLDExtractor(object):
    @classmethod
    def extract(cls, filepath):
        filepath = os.path.abspath(filepath)
        if os.path.isfile(filepath):
            if not os.path.split(filepath)[1].endswith(".zip"):
                raise ValueError(
                    "File not supported:\n\t`%s` is a file but not a zip archive."
                )
            else:
                raise NotImplementedError(
                    "Extraction of zip archives not yet supported"
                )
        subdirectories = [
            os.path.join(filepath, o)
            for o in os.listdir(filepath)
            if o not in DIRECTORIES_TO_IGNORE
            and o not in FILES_TO_IGNORE
            and not IGNORE_ME(o)
        ]
        data = {
            os.path.split(sd)[1]: {
                obj_id: json.load(open(os.path.join(filepath, sd, obj_fp)))
                for obj_id, obj_fp in walk_dir(os.path.join(filepath, sd))
            }
            for sd in subdirectories
        }
        return data
