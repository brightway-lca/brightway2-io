from ..extractors.simapro_csv import SimaProExtractor
from ..strategies import (
    assign_100_percent_allocation_as_reference_product,
    link_based_on_name_and_unit,
    link_biosphere_by_activity_hash,
    split_simapro_name_geo,
)
from .base import ImportBase


class SimaProCSVImporter(ImportBase):
    format_strategies = [
        assign_100_percent_allocation_as_reference_product,
        link_based_on_name_and_unit,
        split_simapro_name_geo,
        link_biosphere_by_activity_hash,
    ]

    def __init__(self, filepath, delimiter=";", name=None):
        self.data = SimaProExtractor.extract(filepath, delimiter, name)
