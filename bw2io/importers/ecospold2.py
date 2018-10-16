# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .base_lci import LCIImporter
from ..errors import MultiprocessingError
from ..extractors import Ecospold2DataExtractor
from ..strategies import (
    assign_single_product_as_activity,
    convert_activity_parameters_to_list,
    create_composite_code,
    delete_exchanges_missing_activity,
    delete_ghost_exchanges,
    drop_temporary_outdated_biosphere_flows,
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
    fix_ecoinvent_flows_pre35,
    update_ecoinvent_locations,
)
from time import time
import os


class SingleOutputEcospold2Importer(LCIImporter):
    format = u"Ecospold2"

    def __init__(self, dirpath, db_name, extractor=Ecospold2DataExtractor,
                 use_mp=True, signal=None):
        self.dirpath = dirpath
        self.db_name = db_name
        self.signal = signal
        self.strategies = [
            normalize_units,
            update_ecoinvent_locations,
            remove_zero_amount_coproducts,
            remove_zero_amount_inputs_with_no_activity,
            remove_unnamed_parameters,
            es2_assign_only_product_with_amount_as_reference_product,
            assign_single_product_as_activity,
            create_composite_code,
            drop_unspecified_subcategories,
            fix_ecoinvent_flows_pre35,
            drop_temporary_outdated_biosphere_flows,
            link_biosphere_by_flow_uuid,
            link_internal_technosphere_by_composite_code,
            delete_exchanges_missing_activity,
            delete_ghost_exchanges,
            remove_uncertainty_from_negative_loss_exchanges,
            fix_unreasonably_high_lognormal_uncertainties,
            set_lognormal_loc_value,
            convert_activity_parameters_to_list,
        ]

        start = time()
        try:
            self.data = extractor.extract(dirpath, db_name, use_mp=use_mp)
        except RuntimeError as e:
            raise MultiprocessingError('Multiprocessing error; re-run using `use_mp=False`'
                            ).with_traceback(e.__traceback__)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
            len(self.data), time() - start))
