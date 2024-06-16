from typing import Optional
from io import StringIO
from pathlib import Path
import functools
from time import time

from bw2data import Database, config
from bw_simapro_csv import SimaProCSV

from ..strategies import (
    assign_only_product_as_production,
    change_electricity_unit_mj_to_kwh,
    convert_activity_parameters_to_list,
    drop_unspecified_subcategories,
    fix_localized_water_flows,
    fix_zero_allocation_products,
    link_iterable_by_fields,
    link_technosphere_based_on_name_unit_location,
    migrate_datasets,
    migrate_exchanges,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    normalize_simapro_biosphere_categories,
    normalize_simapro_biosphere_names,
    normalize_units,
    set_code_by_activity_hash,
    sp_allocate_products,
    split_simapro_name_geo,
    strip_biosphere_exc_locations,
    update_ecoinvent_locations,
)
from ..strategies.simapro import set_lognormal_loc_value_uncertainty_safe
from .base_lci import LCIImporter


class SimaProBlockCSVImporter(LCIImporter):
    format = u"bw_simapro_csv"

    def __init__(
        self,
        path_or_stream: Path | StringIO,
        database_name: Optional[str] = None,
    ):
        start = time()
        data = SimaProCSV(path_or_stream=path_or_stream, database_name=database_name).to_brightway()

        # project_parameters
        # database_parameters

        self.db_name = data.database_name

        self.strategies = [
            normalize_units,
            update_ecoinvent_locations,
            assign_only_product_as_production,
            drop_unspecified_subcategories,
            sp_allocate_products,
            fix_zero_allocation_products,
            split_simapro_name_geo,
            strip_biosphere_exc_locations,
            functools.partial(migrate_datasets, migration="default-units"),
            functools.partial(migrate_exchanges, migration="default-units"),
            functools.partial(set_code_by_activity_hash, overwrite=True),
            change_electricity_unit_mj_to_kwh,
            link_technosphere_based_on_name_unit_location,
            set_lognormal_loc_value_uncertainty_safe,
        ]
        if normalize_biosphere:
            self.strategies.extend(
                [
                    normalize_biosphere_categories,
                    normalize_simapro_biosphere_categories,
                    normalize_biosphere_names,
                    normalize_simapro_biosphere_names,
                    functools.partial(migrate_exchanges, migration="simapro-water"),
                    fix_localized_water_flows,
                ]
            )
        self.strategies.extend(
            [
                functools.partial(
                    link_iterable_by_fields,
                    other=Database(biosphere_db or config.biosphere),
                    kind="biosphere",
                ),
                convert_activity_parameters_to_list,
            ]
        )

    def get_db_name(self):
        candidates = {obj["database"] for obj in self.data}
        if not len(candidates) == 1:
            raise ValueError("Can't determine database name from {}".format(candidates))
        return list(candidates)[0]

    def write_database(self, data=None, name=None, *args, **kwargs):
        db = super(SimaProCSVImporter, self).write_database(data, name, *args, **kwargs)
        # database_parameters[db.name] = self.global_parameters
        db.metadata["simapro import"] = self.metadata
        db._metadata.flush()
        return db

    # def match_ecoinvent3(self, db_name, system_model):
    #     """Link SimaPro transformed names to an ecoinvent 3.X database.

    #     Will only link processes from the given ``system_model``. Available ``system_model``s are:

    #         * apos
    #         * consequential
    #         * cutoff

    #     Matching across system models is possible, but not all processes in one system model exist in other system models.

    #     """
    #     currently_unmatched = self.statistics(False)[2]
    #     func_list = [functools.partial(
    #         sp_match_ecoinvent3_database,
    #         ei3_name=db_name
    #     )]
    #     self.apply_strategies(func_list)
    #     matched = currently_unmatched - self.statistics(False)[2]
    #     print(u"Matched {} exchanges".format(matched))

    def match_ecoinvent2(self, db_name):
        currently_unmatched = self.statistics(False)[2]
        # func_list = [
        #     functools.partial(
        #     sp_detoxify_link_technosphere_by_activity_hash,
        #     external_db_name=db_name
        # )]
        # TODO
        self.apply_strategies(func_list)
        matched = currently_unmatched - self.statistics(False)[2]
        print(u"Matched {} exchanges".format(matched))
