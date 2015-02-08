from __future__ import print_function
from .base import ImportBase
from ..extractors import Ecospold2DataExtractor
from ..strategies import (
    create_composite_code,
    delete_exchanges_missing_activity,
    es2_assign_only_production_with_amount_as_reference_product,
    link_biosphere_by_flow_uuid,
    link_internal_technosphere_by_composite_code,
    remove_zero_amount_coproducts,
)
from time import time


class SingleOutputEcospold2Importer(ImportBase):
    format_strategies = [
        es2_assign_only_production_with_amount_as_reference_product,
        remove_zero_amount_coproducts,
        create_composite_code,
        link_biosphere_by_flow_uuid,
        link_internal_technosphere_by_composite_code,
        delete_exchanges_missing_activity,
    ]
    format = u"Ecospold2"

    def __init__(self, filepath, db_name):
        self.filepath = filepath
        self.db_name = db_name
        start = time()
        self.data = Ecospold2DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
            len(self.data), time() - start))

    def create_biosphere3(self):
        metadata_dir = os.path.join(self.dirpath, "..", "MasterData")
        data = Ecospold2DataExtractor.extract_biosphere_metadata(metadata_dir)
        self.write_database(data, u"biosphere3")


class _Ecospold2Importer(object):
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
    pass
