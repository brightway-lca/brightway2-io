# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .biosphere import (
    drop_unspecified_subcategories,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    strip_biosphere_exc_locations,
)
from .ecospold1_allocation import es1_allocate_multioutput, clean_integer_codes
from .ecospold2 import (
    assign_single_product_as_activity,
    create_composite_code,
    delete_exchanges_missing_activity,
    delete_ghost_exchanges,
    es2_assign_only_product_with_amount_as_reference_product,
    link_biosphere_by_flow_uuid,
    link_internal_technosphere_by_composite_code,
    nuncertainty,
    remove_unnamed_parameters,
    remove_zero_amount_coproducts,
    remove_zero_amount_inputs_with_no_activity,
)
from .generic import (
    assign_only_product_as_production,
    drop_unlinked,
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
    normalize_units,
    set_code_by_activity_hash,
    tupleize_categories,
)
from .lcia import (
    add_activity_hash_code,
    drop_unlinked_cfs,
    set_biosphere_type,
    match_subcategories,
)
from .migrations import (
    migrate_datasets,
    migrate_exchanges,
)
from .simapro import (
    link_technosphere_based_on_name_unit_location,
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
    normalize_simapro_product_units,
    sp_allocate_products,
    split_simapro_name_geo,
)
from . import special
