# -*- coding: utf-8 -*
from __future__ import division, print_function
from ..units import normalize_units
from bw2data import Database, databases, mapping
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import copy
import hashlib
import os
import pprint
import progressbar
import warnings

EMISSIONS = (u"air", u"water", u"soil")
PM_MAPPING = {
    u'reliability': u'reliability',
    u'completeness': u'completeness',
    u'temporalCorrelation': u'temporal correlation',
    u'geographicalCorrelation': u'geographical correlation',
    u'furtherTechnologyCorrelation': u'further technological correlation'
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
                u'name': o.name.text,
                u'unit': normalize_units(o.unitName.text),
                u'id': o.get(u'id')
            }

        fp = os.path.join(dirpath, u"IntermediateExchanges.xml")
        assert os.path.exists(fp), u"Can't find IntermediateExchanges.xml"
        root = objectify.parse(open(fp)).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract_biosphere_metadata(cls, dirpath):
        def extract_metadata(o):
            return {
                u'name': o.name.text,
                u'unit': normalize_units(o.unitName.text),
                u'id': o.get(u'id'),
                u'categories': (
                    o.compartment.compartment.text,
                    o.compartment.subcompartment.text
                )
            }

        fp = os.path.join(dirpath, u"ElementaryExchanges.xml")
        assert os.path.exists(fp), u"Can't find ElementaryExchanges.xml"
        root = objectify.parse(open(fp)).getroot()
        return [extract_metadata(ds) for ds in root.iterchildren()]

    @classmethod
    def extract(cls, dirpath, multioutput=False):
        assert os.path.exists(dirpath)
        filelist = [filename for filename in os.listdir(dirpath)
                    if os.path.isfile(os.path.join(dirpath, filename))
                    and filename.split(".")[-1].lower() == u"spold"
                    ]

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
            data.append(cls.extract_activity(dirpath, filename, multioutput))
            pbar.update(index)
        pbar.finish()

        return recursive_str_to_unicode(data)

    @classmethod
    def condense_multiline_comment(cls, element):
        try:
            return u"\n".join([
                child.text for child in element.iterchildren()
                if child.tag == u"{http://www.EcoInvent.org/EcoSpold02}text"]
            )
        except:
            return u""

    @classmethod
    def extract_activity(cls, dirpath, filename, multioutput=False):
        root = objectify.parse(open(os.path.join(dirpath, filename))).getroot()
        if hasattr(root, u"activityDataset"):
            stem = root.activityDataset
        else:
            stem = root.childActivityDataset

        comments = [
            cls.condense_multiline_comment(getattr2(stem.activityDescription.activity, u"generalComment")),
            (u"Included activities start: ", getattr2(stem.activityDescription.activity, u"includedActivitiesStart").get(u"text")),
            (u"Included activities end: ", getattr2(stem.activityDescription.activity, u"includedActivitiesEnd").get(u"text")),
            (u"Geography: ", cls.condense_multiline_comment(getattr2(
                stem.activityDescription.geography, u"comment"))),
            (u"Technology: ", cls.condense_multiline_comment(getattr2(
                stem.activityDescription.technology, u"comment"))),
            (u"Time period: ", cls.condense_multiline_comment(getattr2(
                stem.activityDescription.timePeriod, u"comment"))),
        ]
        comment = "\n".join([
            (" ".join(x) if isinstance(x, tuple) else x)
            for x in comments
            if (x[1] if isinstance(x, tuple) else x)
        ])

        data = {
            u'name':      stem.activityDescription.activity.activityName.text,
            u'location':  stem.activityDescription.geography.shortname.text,
            u'exchanges': [cls.extract_exchange(exc, multioutput)
                           for exc in stem.flowData.iterchildren()],
            u'linking': {
                u'activity':  stem.activityDescription.activity.get('id'),
                u'filename':  filename,
            },
            u"comment": comment,
        }
        data[u'products'] = [copy.deepcopy(exc)
                             for exc in data[u'exchanges']
                             if exc.get(u'product', 0)
                             ]

        # Multi-output datasets, when allocated, keep all product exchanges,
        # but set some amounts to zero...
        ref_product_candidates = [exc for exc in data[u'products'] if exc[u'amount']]
        # Allocation datasets only have one product actually produced
        assert len(ref_product_candidates) == 1
        data[u"linking"][u'flow'] = ref_product_candidates[0][u'flow']

        # Despite using a million UUIDs, there is actually no unique ID in
        # an ecospold2 dataset. Datasets are uniquely identified by the
        # combination of activity and flow UUIDs.
        data[u'id'] = hashlib.md5(data[u"linking"][u'activity'] +
                                  data[u"linking"][u'flow']).hexdigest()
        data[u'reference product'] = ref_product_candidates[0][u'name']

        # Purge parameters (where `extract_exchange` returns `{}`)
        # and exchanges with `amount` of zero
        # and byproducts (amount of zero),
        # and non-allocated reference products (amount of zero)
        # Multioutput products are kept in 'products'
        data[u'exchanges'] = [
            x for x in data[u'exchanges']
            if x
            and x[u'amount'] != 0
        ]
        return data

    @classmethod
    def abort_exchange(cls, exc):
        exc[u"uncertainty type"] = UndefinedUncertainty.id
        exc[u"loc"] = exc[u"amount"]
        for key in (u"scale", u"shape", u"minimum", u"maximum"):
            if key in exc:
                del exc[key]
        exc[u"comment"] = u"Invalid parameters - set to undefined uncertainty."

    @classmethod
    def extract_exchange(cls, exc, multioutput=False):
        if exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}intermediateExchange":
            flow = "intermediateExchangeId"
            is_biosphere = False
        elif exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}elementaryExchange":
            flow = "elementaryExchangeId"
            is_biosphere = True
        elif exc.tag == u"{http://www.EcoInvent.org/EcoSpold02}parameter":
            return {}
        else:
            print(exc.tag)
            raise ValueError

        # Output group 0 is reference product
        #              2 is by-product
        is_product = (not is_biosphere
                      and hasattr(exc, u"outputGroup")
                      and exc.outputGroup.text == u"0")
        amount = float(exc.get(u'amount'))

        if is_product and amount == 0. and not multioutput:
            # This is system modeled multi-output dataset
            # and a "fake" exchange. It represents a possible
            # output which isn't actualized in this allocation
            # and can therefore be ignored. This shouldn't exist
            # at all, but the data format is not perfect.
            return {}

        data = {
            u'flow': exc.get(flow),
            u'amount': amount,
            u'biosphere': is_biosphere,
            u'product': is_product,
            u'name': exc.name.text,
            u'production volume': float(exc.get("productionVolumeAmount") or 0)
            # 'xml': etree.tostring(exc, pretty_print=True)
        }
        if not is_biosphere:
            # Biosphere flows not produced by an activity
            data[u"activity"] = exc.get(u"activityLinkId")
        if hasattr(exc, u"unitName"):
            data[u'unit'] = normalize_units(exc.unitName.text)

        # Uncertainty fields
        if hasattr(exc, u"uncertainty"):
            unc = exc.uncertainty
            if hasattr(unc, u"pedigreeMatrix"):
                data[u'pedigree'] = dict([(
                    PM_MAPPING[key], int(unc.pedigreeMatrix.get(key)))
                    for key in PM_MAPPING
                ])

            if hasattr(unc, "lognormal"):
                data.update(**{
                    u'uncertainty type': LognormalUncertainty.id,
                    u"loc": float(unc.lognormal.get('mu')),
                    u"scale": float(unc.lognormal.get("varianceWithPedigreeUncertainty")),
                })
                if unc.lognormal.get('variance'):
                    data[u"scale without pedigree"] = float(unc.lognormal.get('variance'))
                if data[u"scale"] <= 0 or data[u"scale"] > 25:
                    cls.abort_exchange(data)
            elif hasattr(unc, u'normal'):
                data.update(**{
                    u"uncertainty type": NormalUncertainty.id,
                    u"loc": float(unc.normal.get('meanValue')),
                    u"scale": float(unc.normal.get('varianceWithPedigreeUncertainty')),
                })
                if unc.normal.get('variance'):
                    data[u"scale without pedigree"] = float(unc.normal.get('variance'))
                if data[u"scale"] <= 0:
                    cls.abort_exchange(data)
            elif hasattr(unc, u'triangular'):
                data.update(**{
                    u'uncertainty type': TriangularUncertainty.id,
                    u'minimum': float(unc.triangular.get('minValue')),
                    u'loc': float(unc.triangular.get('mostLikelyValue')),
                    u'maximum': float(unc.triangular.get('maxValue'))
                })
                if data[u"minimum"] >= data[u"maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, u'uniform'):
                data.update(**{
                    u"uncertainty type": UniformUncertainty.id,
                    u"loc": data[u'amount'],
                    u'minimum': float(unc.uniform.get('minValue')),
                    u'maximum': float(unc.uniform.get('maxValue')),
                })
                if data[u"minimum"] >= data[u"maximum"]:
                    cls.abort_exchange(data)
            elif hasattr(unc, u'undefined'):
                data.update(**{
                    u"uncertainty type": UndefinedUncertainty.id,
                    u"loc": data[u'amount'],
                })
            else:
                raise ValueError(u"Unknown uncertainty type")
        else:
            data.update(**{
                u"uncertainty type": UndefinedUncertainty.id,
                u"loc": data[u'amount'],
            })

        return data
