# -*- coding: utf-8 -*
from __future__ import division, print_function
from .units import normalize_units
from bw2data import Database, databases, mapping
from bw2data.logs import get_io_logger, close_log
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
from stats_arrays.distributions import *
import copy
import hashlib
import os
import pprint
import progressbar
import warnings
from ..extractors.ecospold2 import Ecospold2DataExtractor

EMISSIONS = (u"air", u"water", u"soil")


class Ecospold2Importer(object):
    """Create a new ecospold2 importer object.

    Only exchange numbers are imported, not parameters or formulas.

    .. warning:: You should always check the import log after an ecospold 2 import, because the background database could have missing links that will produce incorrect LCI results.

    Usage: ``Ecospold2Importer(args).importer()``

    Args:
        * *datapath*: Absolute filepath to directory containing the datasets.
        * *metadatapath*: Absolute filepath to the *"MasterData"* directory.
        * *name*: Name of the created database.
        * *multioutput*: Boolean. When importing allocated datasets, include the other outputs in a special *"products"* list.
        * *debug*: Boolean. Include additional debugging information.
        * *new_biosphere*: Boolean. Force writing of a new "biosphere3" database, even if it already exists.

    The data schema for ecospold2 databases is slightly different from ecospold1 databases, as there is some additional data included (only additional data shown here):

    .. code-block:: python

        {
            'linking': {
                'activity': uuid,  # System model-specific activity UUID (location/time specific)
                'flow': uuid,  # System model-specific UUID of the reference product flow (location/time specific)
                'filename': str  # Dataset filename
            },
            'production amount': float,  # Not all activities in ecoinvent 3 are scaled to produce one unit of the reference product
            'products': [
                {exchange_dict},  # List of products. Only has length > 1 if *multioutput* is True. Products which aren't the reference product will have amounts of zero.
            ],
            'reference product': str  # Name of the reference product. Ecospold2 distinguishes between activity and product names.
        }


    Where an exchange in the list of exchanges includes the following additional fields:

    .. code-block:: python

        {
            'production volume': float,  # Yearly production amount in this location and time
            'pedigree matrix': {  # Pedigree matrix values in a structured format
                'completeness': int,
                'further technological correlation': int,
                'geographical correlation': int,
                'reliability': int,
                'temporal correlation': int
            }
        }

    """
    def __init__(self, datapath, name, multioutput=False,
                 debug=False):
        self.datapath = unicode(datapath)
        self.multioutput = multioutput
        self.debug = debug
        self.name = unicode(name)
        self.data = Ecospold2DataExtractor.extract(self.datapath)

    def add_new_biosphere(self):
        data = recursive_str_to_unicode(
            Ecospold2DataExtractor.extract_biosphere_metadata(
                self.metadatapath
            )
        )

        for elem in data:
            elem[u"type"] = "emission" if elem[u'categories'][0] in EMISSIONS \
                else elem[u'categories'][0]
            elem[u"exchanges"] = []

        data = dict([((u"biosphere3", x[u"id"]), x) for x in data])

        if u"biosphere3" in databases:
            del databases[u"biosphere3"]

        print(u"Writing new biosphere database")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database(u"biosphere3")
            db.register(
                format=u"Ecospold2",
            )
            db.write(data)
            db.process()


    def write_database(self):
        data = dict([((self.name, elem[u'id']), elem) for elem in activities])

        assert self.name not in databases, u"This database already exists"
        databases[self.name][u"directory"] = self.datapath
        databases.flush()

        # TODO: This should be a strategy to only link to existing
        print(u"Writing new database")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            db = Database(self.name)
            db.register(
                format=u"Ecospold2",
            )
            db.write(data)

            # Purge any exchanges which link to ghost activities
            # i.e. those not created by the import
            rewrite = False
            for value in data.values():
                for exc in [x for x in value[u'exchanges']
                            if x[u'input'] not in mapping]:
                    rewrite = True
                    self.log.critical(
                        u"Purging unlinked exchange:\nFilename: %s\n%s" %
                        (value[u'linking'][u'filename'],
                         pprint.pformat(exc, indent=2))
                    )
                value[u'exchanges'] = [x for x in value[u'exchanges'] if
                                       x[u'input'] in mapping]

            if rewrite:
                # Rewrite with correct data
                db.write(data)
            db.process()


    def create_database(self, activities):
        print(u"Processing database")
        for elem in activities:
            for exc in elem[u"exchanges"]:
                if exc[u'product']:
                    exc[u'type'] = u'production'
                    exc[u'input'] = (self.name, elem[u'id'])
                    # Activities do not have units, per se - products have units. However,
                    # it is nicer to give the unit of the reference product than nothing.
                    assert "unit" not in elem
                    elem[u"unit"] = exc[u'unit']
                elif exc[u'biosphere']:
                    exc[u'type'] = 'biosphere'
                    exc[u'input'] = (u'biosphere3', exc[u'flow'])
                elif exc[u'activity'] is None:
                    # This exchange wasn't linked correctly by ecoinvent
                    # It is missing the "activityLinkId" attribute
                    # See http://www.ecoinvent.org/database/ecoinvent-version-3/reports-of-changes/known-data-issues/
                    # We ignore it for now, but add attributes to log it later
                    exc[u'input'] = None
                    exc[u'activity filename'] = elem[u"linking"][u'filename']
                    exc[u'activity name'] = elem[u'name']
                    exc[u'type'] = u'unknown'
                    exc[u'unlinked'] = True
                else:
                    # Normal input from technosphere
                    exc[u'type'] = u'technosphere'
                    exc[u'input'] = (
                        self.name,
                        hashlib.md5(exc[u'activity'] + exc[u'flow']).hexdigest()
                    )

            assert "unit" in elem

        # Drop "missing" exchanges
        for elem in activities:
            for exc in [
                    x for x in elem[u"exchanges"]
                    if not x[u'input']
            ]:
                self.log.warning(u"Dropped missing exchange: %s" %
                                 pprint.pformat(exc, indent=2))

            elem[u"exchanges"] = [
                x for x in elem[u"exchanges"]
                if x[u'input']
            ]
