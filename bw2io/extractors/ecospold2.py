# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals, division
from eight import *

from ..utils import es2_activity_hash
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import math
import multiprocessing
import os
import pyprind
import sys


PM_MAPPING = {
    'reliability': 'reliability',
    'completeness': 'completeness',
    'temporalCorrelation': 'temporal correlation',
    'geographicalCorrelation': 'geographical correlation',
    'furtherTechnologyCorrelation': 'further technological correlation'
}

ACTIVITY_TYPES = {
    0: "ordinary transforming activity",
    1: "market activity",
    2: "IO activity",
    3: "Residual activity",
    4: "production mix",
    5: "import activity",
    6: "supply mix",
    7: "export activity",
    8: "re-export activity",
    9: "correction activity",
    10: "market group",
}


def getattr2(obj, attr):
    try:
        return getattr(obj, attr)
    except:
        return {}


TOO_LOW = """Lognormal scale value at or below zero: {}.
Reverting to undefined uncertainty."""
TOO_HIGH = """Lognormal scale value impossibly high: {}.
Reverting to undefined uncertainty."""


class Ecospold2DataExtractor(object):

    @classmethod
    def extract_technosphere_metadata(cls, dirpath):
        def extract_metadata(o):
            return {
                'name': o.name.text,
                'unit': o.unitName.text,
                'id': o.get('id')
            }

        fp = os.path.join(dirpath, "IntermediateExchanges.xml")
        assert os.path.exists(fp), "Can't find IntermediateExchanges.xml"
        root = objectify.parse(open(fp, encoding='utf-8')).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract(cls, dirpath, db_name, use_mp=True):
        assert os.path.exists(dirpath)
        if os.path.isdir(dirpath):
            filelist = [filename for filename in os.listdir(dirpath)
                        if os.path.isfile(os.path.join(dirpath, filename))
                        and filename.split(".")[-1].lower() == "spold"
                        ]
        elif os.path.isfile(dirpath):
            filelist = [dirpath]
        else:
            raise OSError("Can't understand path {}".format(dirpath))

        if sys.version_info < (3, 0):
            use_mp = False

        if use_mp:
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                print("Extracting XML data from {} datasets".format(len(filelist)))
                results = [
                    pool.apply_async(
                        Ecospold2DataExtractor.extract_activity,
                        args=(dirpath, x, db_name)
                    ) for x in filelist
                ]
                data = [p.get() for p in results]
        else:
            pbar = pyprind.ProgBar(len(filelist), title="Extracting ecospold2 files:", monitor=True)

            data = []
            for index, filename in enumerate(filelist):
                data.append(cls.extract_activity(dirpath, filename, db_name))
                pbar.update(item_id = filename[:15])

            print(pbar)

        if sys.version_info < (3, 0):
            print("Converting to unicode")
            return recursive_str_to_unicode(data)
        else:
            return data

    @classmethod
    def condense_multiline_comment(cls, element):
        try:
            return "\n".join([
                child.text for child in element.iterchildren()
                if child.tag == "{http://www.EcoInvent.org/EcoSpold02}text"] + [
                "Image: " + child.text for child in element.iterchildren()
                if child.tag == "{http://www.EcoInvent.org/EcoSpold02}imageUrl"]
            )
        except:
            return ""

    @classmethod
    def extract_activity(cls, dirpath, filename, db_name):
        root = objectify.parse(open(os.path.join(dirpath, filename), encoding='utf-8')).getroot()
        if hasattr(root, "activityDataset"):
            stem = root.activityDataset
        else:
            stem = root.childActivityDataset

        comments = [
            cls.condense_multiline_comment(getattr2(stem.activityDescription.activity, "generalComment")),
            ("Included activities start: ", getattr2(stem.activityDescription.activity, "includedActivitiesStart").get("text")),
            ("Included activities end: ", getattr2(stem.activityDescription.activity, "includedActivitiesEnd").get("text")),
            ("Geography: ", cls.condense_multiline_comment(getattr2(
                stem.activityDescription.geography, "comment"))),
            ("Technology: ", cls.condense_multiline_comment(getattr2(
                stem.activityDescription.technology, "comment"))),
            ("Time period: ", cls.condense_multiline_comment(getattr2(
                stem.activityDescription.timePeriod, "comment"))),
        ]
        comment = "\n".join([
            (" ".join(x) if isinstance(x, tuple) else x)
            for x in comments
            if (x[1] if isinstance(x, tuple) else x)
        ])

        classifications = [(el.classificationSystem.text, el.classificationValue.text)
            for el in stem.activityDescription.iterchildren()
            if el.tag == u'{http://www.EcoInvent.org/EcoSpold02}classification']

        data = {
            "comment": comment,
            "classifications": classifications,
            "activity type": ACTIVITY_TYPES[int(
                stem.activityDescription.activity.get('specialActivityType')
                or 0
            )],
            'activity':  stem.activityDescription.activity.get('id'),
            'database': db_name,
            'exchanges': [cls.extract_exchange(exc)
                           for exc in stem.flowData.iterchildren()
                           if "parameter" not in exc.tag],
            'filename':  filename,
            'location':  stem.activityDescription.geography.shortname.text,
            'name':      stem.activityDescription.activity.activityName.text,
            'parameters': dict([cls.extract_parameter(exc)
                                for exc in stem.flowData.iterchildren()
                                if "parameter" in exc.tag]),
            "authors": {
                "data entry": {
                    "name": stem.administrativeInformation.dataEntryBy.get('personName'),
                    "email": stem.administrativeInformation.dataEntryBy.get('personEmail')
                },
                "data generator": {
                    "name": stem.administrativeInformation.dataGeneratorAndPublication.get('personName'),
                    "email": stem.administrativeInformation.dataGeneratorAndPublication.get('personEmail')
                }
            },
            "type": "process",
        }
        return data

    @classmethod
    def abort_exchange(cls, exc, comment=None):
        comment
        exc["uncertainty type"] = UndefinedUncertainty.id
        exc["loc"] = exc["amount"]
        for key in ("scale", "shape", "minimum", "maximum"):
            if key in exc:
                del exc[key]
        exc["comment"] = exc.get('comment', '')
        if exc['comment']:
            exc['comment'] += '\n'
        exc['comment'] += (
            comment or "Invalid parameters - set to undefined uncertainty."
        )

    @classmethod
    def extract_uncertainty_dict(cls, obj):
        data = {
            'amount': float(obj.get('amount')),
        }
        if obj.get('formula'):
            data['formula'] = obj.get('formula')

        if hasattr(obj, "uncertainty"):
            unc = obj.uncertainty
            if hasattr(unc, "pedigreeMatrix"):
                data['pedigree'] = dict([(
                    PM_MAPPING[key], int(unc.pedigreeMatrix.get(key)))
                    for key in PM_MAPPING
                ])

            if hasattr(unc, "lognormal"):
                data.update({
                    'uncertainty type': LognormalUncertainty.id,
                    "loc": float(unc.lognormal.get('mu')),
                    "scale": math.sqrt(float(unc.lognormal.get("varianceWithPedigreeUncertainty"))),
                })
                if unc.lognormal.get('variance'):
                    data["scale without pedigree"] = math.sqrt(float(unc.lognormal.get('variance')))
                if data["scale"] <= 0:
                    cls.abort_exchange(data, TOO_LOW.format(data['scale']))
                elif data["scale"] > 25:
                    cls.abort_exchange(data, TOO_HIGH.format(data['scale']))
            elif hasattr(unc, 'normal'):
                data.update({
                    "uncertainty type": NormalUncertainty.id,
                    "loc": float(unc.normal.get('meanValue')),
                    "scale": math.sqrt(float(unc.normal.get('varianceWithPedigreeUncertainty'))),
                })
                if unc.normal.get('variance'):
                    data["scale without pedigree"] = math.sqrt(float(unc.normal.get('variance')))
                if data["scale"] <= 0:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'triangular'):
                data.update({
                    'uncertainty type': TriangularUncertainty.id,
                    'minimum': float(unc.triangular.get('minValue')),
                    'loc': float(unc.triangular.get('mostLikelyValue')),
                    'maximum': float(unc.triangular.get('maxValue'))
                })
                if data["minimum"] >= data["maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'uniform'):
                data.update({
                    "uncertainty type": UniformUncertainty.id,
                    "loc": data['amount'],
                    'minimum': float(unc.uniform.get('minValue')),
                    'maximum': float(unc.uniform.get('maxValue')),
                })
                if data["minimum"] >= data["maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'undefined'):
                data.update({
                    "uncertainty type": UndefinedUncertainty.id,
                    "loc": data['amount'],
                })
            else:
                raise ValueError("Unknown uncertainty type")
        else:
            data.update({
                "uncertainty type": UndefinedUncertainty.id,
                "loc": data['amount'],
            })
        return data

    @classmethod
    def extract_parameter(cls, exc):
        name = exc.get("variableName")
        data = {
            'description': exc.name.text,
            'id': exc.get("parameterId"),
        }
        if hasattr(exc, "unitName"):
            data['unit'] = exc.unitName.text
        if hasattr(exc, "comment"):
            data['comment'] = exc.comment.text
        data.update(cls.extract_uncertainty_dict(exc))
        if name is None:
            name = "Unnamed parameter: {}".format(data['id'])
            data['unnamed'] = True
        return name, data

    @classmethod
    def extract_exchange(cls, exc):
        """Process exchange.

        Input groups are:

            1. Materials/fuels
            2. Electricity/Heat
            3. Services
            4. From environment (elementary exchange only)
            5. FromTechnosphere

        Output groups are:

            0. ReferenceProduct
            2. By-product
            3. MaterialForTreatment
            4. To environment (elementary exchange only)
            5. Stock addition

        """
        if exc.tag == "{http://www.EcoInvent.org/EcoSpold02}intermediateExchange":
            flow = "intermediateExchangeId"
            is_biosphere = False
        elif exc.tag == "{http://www.EcoInvent.org/EcoSpold02}elementaryExchange":
            flow = "elementaryExchangeId"
            is_biosphere = True
        else:
            print(exc.tag)
            raise ValueError

        is_product = (hasattr(exc, "outputGroup")
                      and exc.outputGroup.text in ("0", "2"))

        if is_biosphere and is_product:
            raise ValueError("Impossible output group")

        if is_product:
            kind = "production"
        elif is_biosphere:
            kind = "biosphere"
        else:
            kind = "technosphere"

        data = {
            'flow': exc.get(flow),
            'type': kind,
            'name': exc.name.text,
            'production volume': float(exc.get("productionVolumeAmount") or 0)
            # 'xml': etree.tostring(exc, pretty_print=True)
        }
        if not is_biosphere:
            data["activity"] = exc.get("activityLinkId")
        if hasattr(exc, "unitName"):
            data['unit'] = exc.unitName.text
        if hasattr(exc, "comment"):
            data['comment'] = exc.comment.text

        data.update(cls.extract_uncertainty_dict(exc))
        return data
