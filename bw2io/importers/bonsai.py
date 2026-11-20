from bw2io.importers.io import IOImporter
from bw2io.strategies.generic import normalize_units

from bw2io.strategies.bonsai import assign_activity_type


class BonsaiImporter(IOImporter):

    def apply_strategies(self):
        self.products = normalize_units(self.products)
        # differentiate between market and production activities by the code
        self.products = assign_activity_type(self.products)
