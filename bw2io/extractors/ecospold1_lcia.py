# -*- coding: utf-8 -*-
from ..units import normalize_units
from bw2data.logs import get_io_logger, close_log
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
import os
try:
    import progressbar
except ImportError:
    progressbar = None


class Ecospold1LCIAExtractor(object):
    """Extract impact assessment methods and weightings data from ecospold XML format."""
    @classmethod
    def extract(cls, path):
        if os.path.isdir(path):
            files = [os.path.join(path, name) for name in \
                filter(lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]

        if progressbar:
            widgets = [
                progressbar.SimpleProgress(sep="/"), " (",
                progressbar.Percentage(), ') ',
                progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
                progressbar.ETA()
            ]
            pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(files)
                ).start()

        methods_data = []

        for index, filepath in enumerate(files):
            # Note that this is only used for the first root method found in
            # the file
            root = objectify.parse(open(filepath)).getroot()
            for dataset in root.iterchildren():
                methods_data.append(recursive_str_to_unicode(
                    cls.parse_method(dataset, filepath)
                ))
            pbar.update(index) if progressbar else None

        pbar.finish() if progressbar else None
        return methods_data

    @classmethod
    def parse_method(cls, ds, filepath):
        ref_func = ds.metaInformation.processInformation.referenceFunction
        return {
            "exchanges": [cls.parse_cf(o) for o in ds.flowData.iterchildren()],
            "description": ref_func.get("generalComment") or "",
            "filename": filepath,
            "name": (ref_func.get("category"), ref_func.get("subCategory"),
                     ref_func.get("name")),
            "unit": ref_func.get("unit") or "",
        }

    @classmethod
    def parse_cf(cls, cf):
        data = {
            "amount": float(cf.get("meanValue")),
            "categories": (
                cf.get("category"),
                cf.get("subCategory") or None
            ),
            "name": cf.get("name"),
            "unit": normalize_units(cf.get("unit")),
        }
        return data
