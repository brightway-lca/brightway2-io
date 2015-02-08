# -*- coding: utf-8 -*
from ..extractors.simapro_csv import SimaProCSVExtractor
from ..strategies import (
    sp_allocate_products,
    link_based_on_name_and_unit,
    link_biosphere_by_activity_hash,
    split_simapro_name_geo,
)
from .base import ImportBase
from ..utils import load_json_data_file
from time import time

SIMAPRO_SYSTEM_MODELS = {
    "Allocation, cut-off by classification": "cutoff",
    "Allocation, ecoinvent default": "apos",
    "Substitution, consequential, long-term": "consequential",
}


class SimaProCSVImporter(ImportBase):
    format_strategies = [
        sp_allocate_products,
        link_based_on_name_and_unit,
        split_simapro_name_geo,
        link_biosphere_by_activity_hash,
    ]
    format = u"SimaPro CSV"

    def __init__(self, filepath, delimiter=";", name=None):
        start = time()
        self.data = SimaProCSVExtractor.extract(filepath, delimiter, name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))
        if name:
            self.db_name = name
        else:
            self.db_name = self.get_db_name()

    def get_db_name(self):
        candidates = {obj['database'] for obj in self.data}
        if not len(candidates) == 1:
            raise ValueError("Can't determine database name from {}".format(candidates))
        return candidates[0]

    def match_ecoinvent3(self, db_name, system_model=None):
        """Link SimaPro transformed names to an ecoinvent 3.X database.

        Will temporarily load database ``db_name`` into memory.

        If ``system_model``, will only link processes from the given system model. Available ``system_model``s are:

            * apos
            * consequential
            * cutoff

        Correspondence file is from Pré, and has the following fields:

            #. SimaPro name
            #. Ecoinvent flow name
            #. Location
            #. Ecoinvent activity name
            #. System model
            #. SimaPro type

        Where system model is one of:

            * Allocation, cut-off by classification
            * Allocation, ecoinvent default

        And SimaPro type is either ``System terminated`` or ``Unit process``. We always match to unit processes regardless of SimaPro type.

        """
        correspondence = load_json_data_file("simapro-ecoinvent31")
        if system_model:
            assert system_model in {"cutoff", "consequential", "apos"}, \
                "``system mode must be one of: cutoff, consequential, apos"
            pass
        assert db_name in databases, u"Unknown database {}".format(db_name)
        possibles = [{(obj.name): obj.key} for obj in Database(db_name)]
        # TODO: Finish

# TODO: SimaPro8 use EI3 biosphere categories/names?
