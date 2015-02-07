# -*- coding: utf-8 -*
from __future__ import division, print_function
from ..units import normalize_units
from ..utils import es2_activity_hash
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import os
import progressbar

PM_MAPPING = {
    'reliability': 'reliability',
    'completeness': 'completeness',
    'temporalCorrelation': 'temporal correlation',
    'geographicalCorrelation': 'geographical correlation',
    'furtherTechnologyCorrelation': 'further technological correlation'
}


def getattr2(obj, attr):
    try:
        return getattr(obj, attr)
    except:
        return {}


class Ecospold2DataExtractor(object):

    @classmethod
    def extract_technosphere_metadata(cls, dirpath):
        def extract_metadata(o):
            return {
                'name': o.name.text,
                'unit': normalize_units(o.unitName.text),
                'id': o.get('id')
            }

        fp = os.path.join(dirpath, "IntermediateExchanges.xml")
        assert os.path.exists(fp), "Can't find IntermediateExchanges.xml"
        root = objectify.parse(open(fp)).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract_biosphere_metadata(cls, dirpath):
        def extract_metadata(o):
            return {
                'name': o.name.text,
                'unit': normalize_units(o.unitName.text),
                'id': o.get('id'),
                'categories': (
                    o.compartment.compartment.text,
                    o.compartment.subcompartment.text
                )
            }

        fp = os.path.join(dirpath, "ElementaryExchanges.xml")
        assert os.path.exists(fp), "Can't find ElementaryExchanges.xml"
        root = objectify.parse(open(fp)).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract(cls, dirpath):
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

        widgets = [
            progressbar.SimpleProgress(sep="/"), " (",
            progressbar.Percentage(), ') ',
            progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
            progressbar.ETA()
        ]
        pbar = progressbar.ProgressBar(
            widgets=widgets,
            maxval=len(filelist)
        ).start()

        data = []
        for index, filename in enumerate(filelist):
            data.append(cls.extract_activity(dirpath, filename))
            pbar.update(index)
        pbar.finish()

        return recursive_str_to_unicode(data)

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
    def extract_activity(cls, dirpath, filename):
        root = objectify.parse(open(os.path.join(dirpath, filename))).getroot()
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

        data = {
            'name':      stem.activityDescription.activity.activityName.text,
            'location':  stem.activityDescription.geography.shortname.text,
            'exchanges': [cls.extract_exchange(exc)
                           for exc in stem.flowData.iterchildren()
                           if "parameter" not in exc.tag],
            'parameters': [cls.extract_parameter(exc)
                           for exc in stem.flowData.iterchildren() if "parameter" in exc.tag],
            'activity':  stem.activityDescription.activity.get('id'),
            'filename':  filename,
            "comment": comment,
        }
        data['products'] = [exc for exc in data['exchanges']
                             if exc['type'] == 'production']
        # data['id'] = es2_activity_hash(data['activity'], data['flow'])

        return data

    @classmethod
    def abort_exchange(cls, exc):
        exc["uncertainty type"] = UndefinedUncertainty.id
        exc["loc"] = exc["amount"]
        for key in ("scale", "shape", "minimum", "maximum"):
            if key in exc:
                del exc[key]
        exc["comment"] = exc.get('comment', '') + "; Invalid parameters - set to undefined uncertainty."

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
                data.update(**{
                    'uncertainty type': LognormalUncertainty.id,
                    "loc": float(unc.lognormal.get('mu')),
                    "scale": float(unc.lognormal.get("varianceWithPedigreeUncertainty")),
                })
                if unc.lognormal.get('variance'):
                    data["scale without pedigree"] = float(unc.lognormal.get('variance'))
                if data["scale"] <= 0 or data["scale"] > 25:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'normal'):
                data.update(**{
                    "uncertainty type": NormalUncertainty.id,
                    "loc": float(unc.normal.get('meanValue')),
                    "scale": float(unc.normal.get('varianceWithPedigreeUncertainty')),
                })
                if unc.normal.get('variance'):
                    data["scale without pedigree"] = float(unc.normal.get('variance'))
                if data["scale"] <= 0:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'triangular'):
                data.update(**{
                    'uncertainty type': TriangularUncertainty.id,
                    'minimum': float(unc.triangular.get('minValue')),
                    'loc': float(unc.triangular.get('mostLikelyValue')),
                    'maximum': float(unc.triangular.get('maxValue'))
                })
                if data["minimum"] >= data["maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'uniform'):
                data.update(**{
                    "uncertainty type": UniformUncertainty.id,
                    "loc": data['amount'],
                    'minimum': float(unc.uniform.get('minValue')),
                    'maximum': float(unc.uniform.get('maxValue')),
                })
                if data["minimum"] >= data["maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, 'undefined'):
                data.update(**{
                    "uncertainty type": UndefinedUncertainty.id,
                    "loc": data['amount'],
                })
            else:
                raise ValueError("Unknown uncertainty type")
        else:
            data.update(**{
                "uncertainty type": UndefinedUncertainty.id,
                "loc": data['amount'],
            })
        return data

    @classmethod
    def extract_parameter(cls, exc):
        data = {
            'name': exc.get("variableName"),
            'description': exc.name.text,
        }
        if hasattr(exc, "unitName"):
            data['unit'] = normalize_units(exc.unitName.text)
        if hasattr(exc, "comment"):
            data['comment'] = exc.comment.text
        data.update(**cls.extract_uncertainty_dict(exc))
        return data

    @classmethod
    def extract_exchange(cls, exc):
        if exc.tag == "{http://www.EcoInvent.org/EcoSpold02}intermediateExchange":
            flow = "intermediateExchangeId"
            is_biosphere = False
        elif exc.tag == "{http://www.EcoInvent.org/EcoSpold02}elementaryExchange":
            flow = "elementaryExchangeId"
            is_biosphere = True
        else:
            print(exc.tag)
            raise ValueError

        # Output group 0 is reference product
        #              2 is by-product
        is_product = (not is_biosphere
                      and hasattr(exc, "outputGroup")
                      and exc.outputGroup.text in ("0", "2"))

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
            data['unit'] = normalize_units(exc.unitName.text)
        if hasattr(exc, "comment"):
            data['comment'] = exc.comment.text

        data.update(**cls.extract_uncertainty_dict(exc))
        return data
