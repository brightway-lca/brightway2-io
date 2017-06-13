# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .base_lci import LCIImporter
from ..extractors import Ecospold2DataExtractor
from ..strategies import (
    assign_single_product_as_activity,
    create_composite_code,
    delete_exchanges_missing_activity,
    delete_ghost_exchanges,
    drop_unspecified_subcategories,
    es2_assign_only_product_with_amount_as_reference_product,
    fix_unreasonably_high_lognormal_uncertainties,
    link_biosphere_by_flow_uuid,
    link_internal_technosphere_by_composite_code,
    normalize_units,
    remove_uncertainty_from_negative_loss_exchanges,
    remove_unnamed_parameters,
    remove_zero_amount_coproducts,
    remove_zero_amount_inputs_with_no_activity,
    set_lognormal_loc_value,
)
from time import time
import os


class SingleOutputEcospold2Importer(LCIImporter):
    format = u"Ecospold2"

    def __init__(self, dirpath, db_name):
        self.dirpath = dirpath
        self.db_name = db_name
        self.strategies = [
            normalize_units,
            remove_zero_amount_coproducts,
            remove_zero_amount_inputs_with_no_activity,
            remove_unnamed_parameters,
            es2_assign_only_product_with_amount_as_reference_product,
            assign_single_product_as_activity,
            create_composite_code,
            drop_unspecified_subcategories,
            link_biosphere_by_flow_uuid,
            link_internal_technosphere_by_composite_code,
            delete_exchanges_missing_activity,
            delete_ghost_exchanges,
            remove_uncertainty_from_negative_loss_exchanges,
            fix_unreasonably_high_lognormal_uncertainties,
            set_lognormal_loc_value,
        ]

        start = time()
        self.data = Ecospold2DataExtractor.extract(dirpath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
            len(self.data), time() - start))


class _Ecospold2Importer(object):
    """Create a new ecospold2 importer object.

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
    pass
