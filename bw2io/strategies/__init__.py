# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

__all__ = [
    "add_activity_hash_code",
    "add_database_name",
    "assign_only_product_as_production",
    "assign_single_product_as_activity",
    "change_electricity_unit_mj_to_kwh",
    "clean_integer_codes",
    "convert_activity_parameters_to_list",
    "convert_uncertainty_types_to_integers",
    "create_composite_code",
    "csv_add_missing_exchanges_section",
    "csv_drop_unknown",
    "csv_numerize",
    "csv_restore_booleans",
    "csv_restore_tuples",
    "delete_exchanges_missing_activity",
    "delete_ghost_exchanges",
    "delete_integer_codes",
    "drop_falsey_uncertainty_fields_but_keep_zeros",
    "drop_temporary_outdated_biosphere_flows",
    "drop_unlinked",
    "drop_unlinked_cfs",
    "drop_unspecified_subcategories",
    "es1_allocate_multioutput",
    "es2_assign_only_product_with_amount_as_reference_product",
    "fix_ecoinvent_flows_pre35",
    "fix_localized_water_flows",
    "fix_unreasonably_high_lognormal_uncertainties",
    "fix_zero_allocation_products",
    "link_biosphere_by_flow_uuid",
    "link_internal_technosphere_by_composite_code",
    "link_iterable_by_fields",
    "link_technosphere_based_on_name_unit_location",
    "link_technosphere_by_activity_hash",
    "match_subcategories",
    "migrate_datasets",
    "migrate_exchanges",
    "normalize_biosphere_categories",
    "normalize_biosphere_names",
    "normalize_simapro_biosphere_categories",
    "normalize_simapro_biosphere_names",
    "normalize_units",
    "remove_uncertainty_from_negative_loss_exchanges",
    "remove_unnamed_parameters",
    "remove_zero_amount_coproducts",
    "remove_zero_amount_inputs_with_no_activity",
    "set_biosphere_type",
    "set_code_by_activity_hash",
    "set_lognormal_loc_value",
    "sp_allocate_products",
    "special",
    "split_simapro_name_geo",
    "strip_biosphere_exc_locations",
    "tupleize_categories",
    "update_ecoinvent_locations",
]


from .biosphere import (
    drop_unspecified_subcategories,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    strip_biosphere_exc_locations,
)
from .csv import (
    csv_add_missing_exchanges_section,
    csv_drop_unknown,
    csv_numerize,
    csv_restore_booleans,
    csv_restore_tuples,
)
from .ecospold1_allocation import (
    clean_integer_codes,
    delete_integer_codes,
    es1_allocate_multioutput,
)
from .ecospold2 import (
    assign_single_product_as_activity,
    create_composite_code,
    delete_exchanges_missing_activity,
    delete_ghost_exchanges,
    drop_temporary_outdated_biosphere_flows,
    es2_assign_only_product_with_amount_as_reference_product,
    fix_unreasonably_high_lognormal_uncertainties,
    link_biosphere_by_flow_uuid,
    link_internal_technosphere_by_composite_code,
    remove_uncertainty_from_negative_loss_exchanges,
    remove_unnamed_parameters,
    remove_zero_amount_coproducts,
    remove_zero_amount_inputs_with_no_activity,
    set_lognormal_loc_value,
    fix_ecoinvent_flows_pre35,
)
from .generic import (
    add_database_name,
    assign_only_product_as_production,
    convert_activity_parameters_to_list,
    convert_uncertainty_types_to_integers,
    drop_falsey_uncertainty_fields_but_keep_zeros,
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
    rationalize_method_names,
    set_biosphere_type,
    match_subcategories,
)
from .migrations import (
    migrate_datasets,
    migrate_exchanges,
)
from .simapro import (
    change_electricity_unit_mj_to_kwh,
    fix_localized_water_flows,
    fix_zero_allocation_products,
    link_technosphere_based_on_name_unit_location,
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
    sp_allocate_products,
    split_simapro_name_geo,
)
from .locations import update_ecoinvent_locations
from . import special
