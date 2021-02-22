# -*- coding: utf-8 -*-
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
import os
import pyprind
import sys


def _to_unicode(data):
    if sys.version_info < (3, 0):
        return recursive_str_to_unicode(data)
    else:
        return data


class Ecospold1LCIAExtractor(object):
    """Extract impact assessment methods and weightings data from ecospold XML format."""

    @classmethod
    def extract(cls, path):
        if os.path.isdir(path):
            files = [
                os.path.join(path, name)
                for name in os.listdir(path)
                if name[-4:].lower() == ".xml"
            ]
        else:
            files = [path]

        pbar = pyprind.ProgBar(
            len(files), title="Extracting ecospold1 files:", monitor=True
        )

        methods_data = []

        for filepath in files:
            # Note that this is only used for the first root method found in
            # the file
            root = objectify.parse(open(filepath, encoding="utf-8")).getroot()
            for dataset in root.iterchildren():
                methods_data.append(_to_unicode(cls.parse_method(dataset, filepath)))
            pbar.update(item_id=filename[:15])
        print(pbar)
        return methods_data

    @classmethod
    def parse_method(cls, ds, filepath):
        ref_func = ds.metaInformation.processInformation.referenceFunction
        return {
            "exchanges": [cls.parse_cf(o) for o in ds.flowData.iterchildren()],
            "description": ref_func.get("generalComment") or "",
            "filename": filepath,
            "name": (
                ref_func.get("category"),
                ref_func.get("subCategory"),
                ref_func.get("name"),
            ),
            "unit": ref_func.get("unit") or "",
        }

    @classmethod
    def parse_cf(cls, cf):
        data = {
            "amount": float(cf.get("meanValue")),
            "categories": (cf.get("category"), cf.get("subCategory") or None),
            "name": cf.get("name"),
            "unit": cf.get("unit"),
        }
        return data
