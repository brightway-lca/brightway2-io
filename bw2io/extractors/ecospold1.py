# -*- coding: utf-8 -*
from __future__ import division, print_function
from ..units import normalize_units
from ..utils import activity_hash
from bw2data import Database, mapping, config, databases
from bw2data.logs import get_io_logger
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import copy
import math
import numpy as np
import os
import progressbar

BIOSPHERE = ("air", "water", "soil", "resource", "final-waste-flow")  # Waste flow from SimaPro

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
    def extract(cls, path, log):
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
                data.append(cls.process_dataset(dataset, filename))

            pbar.update(index)
        pbar.finish()
        return recursive_str_to_unicode(data)

    @classmethod
    def process_dataset(cls, dataset, filename):
        ref_func = dataset.metaInformation.processInformation.\
            referenceFunction
        comments = [
            ref_func.get("generalComment"),
            ref_func.get("includedProcesses"),
            (u"Location: ", dataset.metaInformation.processInformation.geography.get("text")),
            (u"Technology: ", dataset.metaInformation.processInformation.technology.get("text")),
            (u"Time period: ", getattr2(dataset.metaInformation.processInformation, "timePeriod").get("text")),
            (u"Production volume: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("productionVolume")),
            (u"Sampling: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("samplingProcedure")),
            (u"Extrapolations: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("extrapolations")),
            (u"Uncertainty: ", getattr2(dataset.metaInformation.modellingAndValidation, "representativeness").get("uncertaintyAdjustments")),
        ]
        comment = "\n".join([
            (" ".join(x) if isinstance(x, tuple) else x)
            for x in comments
            if (x[1] if isinstance(x, tuple) else x)
        ])

        data = {
            u"name": ref_func.get("name").strip(),
            u"type": u"process",
            u"categories": [ref_func.get("category"), ref_func.get(
                "subCategory")],
            u"location": dataset.metaInformation.processInformation.\
                geography.get("location"),
            u"code": int(dataset.get("number")),
            u"unit": normalize_units(ref_func.get("unit")),
            u"exchanges": cls.process_exchanges(dataset),
            u"comment": comment,
            u"filename": filename,
        }
        # Convert ("foo", "unspecified") to ("foo",)
        while data["categories"] and data["categories"][-1] in (
                "unspecified", None):
            data[u"categories"] = data[u"categories"][:-1]
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
            u"reference": int(exc.get("referenceToCoProduct")),
            u"fraction": float(exc.get("fraction")),
            u"exchanges": [int(c.text) for c in exc.iterchildren()]
        }

    @classmethod
    def process_exchange(cls, exc, dataset):
        data = {
            "code": int(exc.get("number")),
            "categories": (exc.get("category"), exc.get("subCategory")),
            "location": exc.get("location"),
            "unit": normalize_units(exc.get("unit")),
            "name": exc.get("name").strip()
        }

        try:
            data["group"] = int(exc.getchildren()[0].text)
        except:
            pass

        # Convert ("foo", "unspecified") to ("foo",)
        while data["matching"]["categories"] and \
                data["matching"]["categories"][-1] in ("unspecified", None):
            data["matching"]["categories"] = \
                data["matching"]["categories"][:-1]

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
            if np.isnan(data['scale']):
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
