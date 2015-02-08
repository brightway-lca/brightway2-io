from ..extractors.simapro_csv import SimaProCSVExtractor
from ..strategies import (
    sp_allocate_products,
    link_based_on_name_and_unit,
    link_biosphere_by_activity_hash,
    split_simapro_name_geo,
)
from .base import ImportBase


class SimaProCSVImporter(ImportBase):
    format_strategies = [
        sp_allocate_products,
        link_based_on_name_and_unit,
        split_simapro_name_geo,
        link_biosphere_by_activity_hash,
    ]

    def __init__(self, filepath, delimiter=";", name=None):
        self.data = SimaProCSVExtractor.extract(filepath, delimiter, name)

    def match_ecoinvent3(self, db_name, system_model=None):
        """Link SimaPro transformed names to an ecoinvent 3.X database.

        Will temporarily load database ``db_name`` into memory.

        If ``system_model``, will only link processes from the given system model. Available ``system_model``s are:

            * apos
            * consequential
            * cutoff

        """
        if system_model:
            assert system_model in {"cutoff", "consequential", "apos"}, \
                "``system mode must be one of: cutoff, consequential, apos"

# TODO: SimaPro8 use EI3 biosphere categories/names?
