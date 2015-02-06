from ..extractors.simapro_csv import SimaProExtractor
from ..strategies.simapro import (
    assign_first_product_as_reference_product,
    assign_100_percent_allocation_as_reference_product,
    link_based_on_name,
    split_simapro_name_geo,
)


class SimaProCSVImporter(object):
    default_strategies = [
        assign_first_product_as_reference_product,
        assign_100_percent_allocation_as_reference_product,
        link_based_on_name,
        split_simapro_name_geo,
    ]

    def __init__(self, filepath, delimiter=";", name=None):
        self.data = SimaProExtractor.extract(filepath, delimiter, name)

    def apply_strategies(self):
        for func in self.default_strategies:
            self.data = func(self.data)
