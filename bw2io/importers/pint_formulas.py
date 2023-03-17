# -*- coding: utf-8 -*-
from functools import partial

from bw2data import Database, config
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    parameters,
    Interpreter,
)

from ..strategies.generic import (
    add_database_name,
    convert_activity_parameters_to_list,
    set_code_by_activity_hash,
    assign_default_location,
    overwrite_exchange_field_values
)
from ..strategies.pint_formulas import (
    add_dummy_amounts,
    add_dummy_inputs,
    delete_dummy_inputs,
    link_activities_to_database,
    convert_exchange_unit_to_input_unit,
)
from ..utils import DEFAULT_FIELDS, HidePrint, ExchangeLinker
from .base_lci import LCIImporter

# replace linking field "unit" by "dimensionality"
MATCHING_FIELDS = tuple(f if f != "unit" else "dimensionality" for f in DEFAULT_FIELDS)
ExchangeLinker.field_funcs[
    "dimensionality"
] = lambda act, field: Interpreter().get_unit_dimensionality(act.get("unit", ""))


class PintFormulasImporter(LCIImporter):
    format = "pint formula importer"

    def __init__(
        self,
        db_name,
        data,
        db_params=None,
    ):
        super().__init__(db_name=db_name)
        self.data = data
        self.database_parameters = db_params
        self._evaluate_formulas()
        self._add_strategies()
        self.write_database = partial(
            self.write_database, delete_existing=True, activate_parameters=True
        )

    def _load_activities_from_disk(self):
        self.data = list(Database(self.db_name).load().values())

    def _load_parameters_from_disk(self):
        self.database_parameters = [
            {"name": k, **v} for k, v in DatabaseParameter.load(self.db_name).items()
        ]
        for act in self.data:
            group_name = f"{self.db_name}:{act['code']}"
            act["parameters"] = ActivityParameter.load(group_name)

    def _evaluate_formulas(self, verbose=False):
        """
        Solves the system of equation defined by project, database and activity parameters and reads the resulting
        amounts and units back into `self.data`.
        """

        # switch off printing
        if not verbose:
            HidePrint.hide()
            config.is_test = True

        # need to write database once to activate parameters
        self.apply_strategies(
            [
                partial(add_database_name, name=self.db_name),
                partial(set_code_by_activity_hash, overwrite=False),
                partial(add_dummy_amounts, amount=0, overwrite=False),
                partial(add_dummy_inputs, overwrite=False),
            ]
        )
        self.write_database(delete_existing=True, activate_parameters=True)

        # read db data (solved units and amounts) back into data
        parameters.recalculate()
        self._load_activities_from_disk()
        self._load_parameters_from_disk()

        # delete dummy inputs
        self.apply_strategy(delete_dummy_inputs)

        # reactivate printing
        if not verbose:
            HidePrint.show()
            config.is_test = False

    def _get_databases(self):
        """
        Goes through all exchanges and collects the database field values.
        """
        databases = {None}
        for act in self.data:
            for exc in act.get("exchanges", []):
                databases.add(exc.get("database"))
        return databases

    def _add_strategies(self):
        """
        Add strategies for linking exchanges to activities, converting units, etc.
        """
        # one linking strategy for each database mentioned in the exchange data
        databases = self._get_databases()
        strategies = [
            partial(
                link_activities_to_database,
                other=Database(db_name) if db_name else None,
                relink=False,
                fields=MATCHING_FIELDS,
            )
            for db_name in databases
        ]
        # add base strategies
        self.strategies = strategies + self.strategies
        # other
        self.strategies += [
            partial(convert_activity_parameters_to_list),
            partial(convert_exchange_unit_to_input_unit),
            partial(
                overwrite_exchange_field_values,
                fields=DEFAULT_FIELDS,
            ),
            partial(
                assign_default_location,
                default_loc="GLO",
                overwrite=False,
            )
        ]
