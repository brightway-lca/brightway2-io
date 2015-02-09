# -*- coding: utf-8 -*
from __future__ import division, print_function
from ..units import normalize_units
from ..utils import activity_hash
from bw2data import Database, mapping, config, databases
from bw2data.logs import get_io_logger, close_log
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import copy
import math
import numpy as np
import os
import progressbar


widgets = [
    progressbar.SimpleProgress(sep="/"), " (",
    progressbar.Percentage(), ') ',
    progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
    progressbar.ETA()
]


def getattr2(obj, attr):
    try:
        return getattr(obj, attr)
    except:
        return {}


class Ecospold1DataExtractor(object):
    @classmethod
    def extract(cls, path, db_name):
        log, logfile = get_io_logger("Ecospold1")
        # TODO: Log import job

        data = []
        if os.path.isdir(path):
            files = [os.path.join(path, y) for y in filter(
                lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]

        if not files:
            raise OSError("Provided path doesn't appear to have any XML files")

        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(files)
            ).start()

        for index, filename in enumerate(files):
            root = objectify.parse(open(filename)).getroot()

            if root.tag not in (
                    '{http://www.EcoInvent.org/EcoSpold01}ecoSpold',
                    'ecoSpold'):
                # Unrecognized file type
                log.critical(u"skipping %s - no ecoSpold element" % filename)
                continue

            for dataset in root.iterchildren():
                data.append(cls.process_dataset(dataset, filename, db_name))

            pbar.update(index)
        pbar.finish()

        close_log(log)

        print(u"Converting to unicode")
        return recursive_str_to_unicode(data)

    @classmethod
    def process_dataset(cls, dataset, filename, db_name):
        ref_func = dataset.metaInformation.processInformation.\
            referenceFunction
        comments = [
            ref_func.get("generalComment"),
            ref_func.get("includedProcesses"),
            ("Location: ", dataset.metaInformation.processInformation.geography.get("text")),
            ("Technology: ", dataset.metaInformation.processInformation.technology.get("text")),
            ("Time period: ", getattr2(dataset.metaInformation.processInformation, "timePeriod").get("text")),
            ("Production volume: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("productionVolume")),
            ("Sampling: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("samplingProcedure")),
            ("Extrapolations: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("extrapolations")),
            ("Uncertainty: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("uncertaintyAdjustments")),
        ]
        comment = "\n".join([
            (" ".join(x) if isinstance(x, tuple) else x)
            for x in comments
            if (x[1] if isinstance(x, tuple) else x)
        ])

        data = {
            "categories": [ref_func.get("category"), ref_func.get(
                "subCategory")],
            "code": int(dataset.get("number")),
            "comment": comment,
            "database": db_name,
            "exchanges": cls.process_exchanges(dataset),
            "filename": filename,
            "location": dataset.metaInformation.processInformation.\
                geography.get("location"),
            "name": ref_func.get("name").strip(),
            "type": "process",
            "unit": normalize_units(ref_func.get("unit")),
        }
        # Convert ("foo", "unspecified") to ("foo",)
        while data["categories"] and data["categories"][-1] in (
                "unspecified", None):
            data["categories"] = data["categories"][:-1]

        allocation_exchanges = [exc for exc in data['exchanges']
                                if exc.get('reference')]

        if allocation_exchanges:
            data['allocations'] = allocation_exchanges
            data['exchanges'] = [exc for exc in data['exchanges']
                                 if exc.get('type')]

        data['products'] = [exc for exc in data['exchanges']
                            if exc['type'] == 'production']
        return data

    @classmethod
    def process_exchanges(cls, dataset):
        data = []
        # Skip definitional exchange - we assume this already
        for exc in dataset.flowData.iterchildren():
            if exc.tag in (
                    "{http://www.EcoInvent.org/EcoSpold01}exchange",
                    "exchange"):
                data.append(cls.process_exchange(exc, dataset))
            elif exc.tag in (
                    "{http://www.EcoInvent.org/EcoSpold01}allocation",
                    "allocation"):
                data.append(cls.process_allocation(exc, dataset))
            else:
                raise ValueError("Flow data type %s not understood" % exc.tag)
        return data

    @classmethod
    def process_allocation(cls, exc, dataset):
        return {
            "reference": int(exc.get("referenceToCoProduct")),
            "fraction": float(exc.get("fraction")),
            "exchanges": [int(c.text) for c in exc.iterchildren()]
        }

    @classmethod
    def process_exchange(cls, exc, dataset):
        """Process exchange.

        Input groups are:

            1. Materials/fuels
            2. Electricity/Heat
            3. Services
            4. FromNature
            5. FromTechnosphere

        Output groups are:

            0. Reference product
            1. Include avoided product system
            2. Allocated byproduct
            3. Waste to treatment
            4. ToNature

        A single-output process will have one output group 0; A MO process will have multiple output group 2s. Output groups 1 and 3 are not used in ecoinvent.
        """
        if hasattr(exc, "outputGroup"):
            if exc.outputGroup.text in {"0", "2"}:
                kind = "production"
            elif exc.outputGroup.text == "1":
                kind = "substitution"
            elif exc.outputGroup.text == "4":
                kind = "biosphere"
            else:
                raise ValueError(u"Can't understand output group {}".format(
                    exc.outputGroup.text))
        else:
            if exc.inputGroup.text in {"1", "2", "3", "5"}:
                kind = "technosphere"
            elif exc.inputGroup.text == "4":
                kind = "biosphere"  # Resources
            else:
                raise ValueError(u"Can't understand input group {}".format(
                    exc.inputGroup.text))

        data = {
            "code": int(exc.get("number")),
            "categories": (exc.get("category"), exc.get("subCategory")),
            "location": exc.get("location"),
            "unit": normalize_units(exc.get("unit")),
            "name": exc.get("name").strip(),
            "type": kind
        }

        # Convert ("foo", "unspecified") to ("foo",)
        while data["categories"] and \
                data["categories"][-1] in ("unspecified", None):
            data["categories"] = \
                data["categories"][:-1]

        if exc.get("generalComment"):
            data["comment"] = exc.get("generalComment")
        return cls.process_uncertainty_fields(exc, data)

    @classmethod
    def process_uncertainty_fields(cls, exc, data):
        uncertainty = int(exc.get("uncertaintyType", 0))

        def floatish(x):
            try:
                return float(x)
            except:
                return np.NaN

        mean = floatish(exc.get("meanValue"))
        min_ = floatish(exc.get("minValue"))
        max_ = floatish(exc.get("maxValue"))
        sigma = floatish(exc.get("standardDeviation95"))

        if uncertainty == 1 and sigma in (0, 1):
            # Bad data
            uncertainty = 0

        if uncertainty == 1:
            # Lognormal
            data.update({
                'uncertainty type': LognormalUncertainty.id,
                'amount': float(mean),
                'loc': np.log(np.abs(mean)),
                'scale': math.log(math.sqrt(float(sigma))),
                'negative': mean < 0,
            })
            if np.isnan(data['scale']) or mean == 0:
                # Bad data
                data['uncertainty type'] = UndefinedUncertainty.id
                data['loc'] = data['amount']
                del data["scale"]
        elif uncertainty == 2:
            # Normal
            data.update({
                'uncertainty type': NormalUncertainty.id,
                'amount': float(mean),
                'loc': float(mean),
                'scale': float(sigma) / 2
            })
        elif uncertainty == 3:
            # Triangular
            data.update({
                'uncertainty type': TriangularUncertainty.id,
                'minimum': float(min_),
                'maximum': float(max_)
            })
            # Sometimes this isn't included (though it SHOULD BE)
            if exc.get("mostLikelyValue"):
                mode = floatish(exc.get("mostLikelyValue"))
                data['amount'] = data['loc'] = mode
            else:
                data['amount'] = data['loc'] = float(mean)
        elif uncertainty == 4:
            # Uniform
            data.update({
                'uncertainty type': UniformUncertainty.id,
                'amount': float(mean),
                'minimum': float(min_),
                'maximum': float(max_)
                })
        else:
            # None
            data.update({
                'uncertainty type': UndefinedUncertainty.id,
                'amount': float(mean),
                'loc': float(mean),
            })
        return data
