from ..extractors import Exiobase3MonetaryDataExtractor
from ..strategies.exiobase import (
    rename_exiobase_co2_eq_flows,
    normalize_units,
    add_stam_labels,
    get_exiobase_biosphere_correspondence,
    add_biosphere_ids,
    remove_numeric_codes,
    add_product_ids,
)
from .base_lci import LCIImporter
from bw2data import Database, config, databases, get_activity, methods, Method
from bw2data.backends.iotable import IOTableBackend
import itertools


class Exiobase3MonetaryImporter(LCIImporter):
    format = "Exiobase 3"

    def __init__(self, dirpath, db_name, ignore_small_balancing_corrections=True):
        self.strategies = []
        self.dirpath = dirpath
        self.db_name = db_name
        self.products = Exiobase3MonetaryDataExtractor.get_products(dirpath)
        self.techosphere_iterator = Exiobase3MonetaryDataExtractor.get_technosphere_iterator(
            dirpath, len(self.products), ignore_small_balancing_corrections
        )
        self.flows = Exiobase3MonetaryDataExtractor.get_flows(dirpath)
        self.biosphere_iterator = Exiobase3MonetaryDataExtractor.get_biosphere_iterator(
            dirpath, ignore_small_balancing_corrections
        )
        self.biosphere_correspondence = get_exiobase_biosphere_correspondence()

    def apply_strategy(self, *args, **kwargs):
        raise NotImplementedError(
            "Not present for IO imports, which have multiple data attributes"
        )

    def add_unlinked_flows_to_new_biosphere_database(self, biosphere_name=None):
        biosphere_name = biosphere_name or self.db_name + " biosphere"
        db = Database(biosphere_name)

        data = {
            (biosphere_name, o["exiobase name"]): {
                "name": o["exiobase name"],
                "unit": o["exiobase unit"],
                "categories": (o["ecoinvent category"],),
                "comment": o["comment"],
                "exchanges": [],
            }
            for o in self.biosphere_correspondence
            if o["new flow"]
        }

        if biosphere_name not in databases:
            db.register(format="EXIOBASE 3 New Biosphere", filepath=str(self.dirpath))

        db.write(data)
        return biosphere_name

    def write_activities_as_database(self):
        db = IOTableBackend(self.db_name)
        data = {
            (self.db_name, "{}|{}".format(o["name"], o["location"])): {
                "name": o["name"],
                "reference product": o["name"],
                "location": o["location"],
                "unit": o["unit"],
                "production volume": o["production volume"],
                "stam": o["stam"],
                "exchanges": [],
            }
            for o in self.products
        }
        if self.db_name not in databases:
            db.register(format="EXIOBASE 3", filepath=str(self.dirpath))
        db.write(data)

    def patch_lcia_methods(self, new_biosphere):
        flows = ["PFC (CO2-eq)", "HFC (CO2-eq)"]

        for flow in flows:
            assert get_activity((new_biosphere, flow))

        new_data = [((new_biosphere, flow), 1) for flow in flows]
        count = 0

        for name, metadata in methods.items():
            if metadata.get("unit") == "kg CO2-Eq":
                count += 1
                obj = Method(name)
                data = obj.load()
                data.extend(new_data)
                obj.write(data)

        print(f"Patched {count} LCIA methods with unit 'kg CO2-Eq'")

    def apply_strategies(self, biosphere=None):
        normalize_units(self.products)
        normalize_units(self.biosphere_correspondence, "exiobase unit")
        rename_exiobase_co2_eq_flows(self.biosphere_correspondence)
        remove_numeric_codes(self.products)
        add_stam_labels(self.products)

    def write_database(self, biosphere=None):
        new_biosphere = self.add_unlinked_flows_to_new_biosphere_database()
        main_biosphere = biosphere or config.biosphere
        print(
            "Created new database for EXIOBASE-specific biosphere flows: {}".format(
                new_biosphere
            )
        )
        add_biosphere_ids(
            self.biosphere_correspondence, [new_biosphere, main_biosphere]
        )

        self.write_activities_as_database()
        print("Created database of EXIOBASE activity metadata")
        add_product_ids(self.products, self.db_name)

        self.patch_lcia_methods(new_biosphere)
        print("Patching LCIA methods with EXIOBASE flows")

        product_mapping = {(o["name"], o["location"]): o["id"] for o in self.products}
        biosphere_mapping = {
            o["exiobase name"]: o["id"]
            for o in self.biosphere_correspondence
            if "id" in o
        }
        biosphere_scales = {
            o["exiobase name"]: float(o["scale factor"])
            for o in self.biosphere_correspondence
            if "id" in o
        }

        technosphere = itertools.chain(
            (
                {
                    "row": product_mapping[x],
                    "col": product_mapping[y],
                    "amount": z,
                    "flip": True,
                    "uncertainty_type": 0,
                }
                for x, y, z in self.techosphere_iterator
            ),
            (
                {"row": x, "col": x, "amount": 1, "flip": False, "uncertainty_type": 0}
                for x in product_mapping.values()
            ),
        )
        biosphere = (
            {
                "row": biosphere_mapping[x],
                "col": product_mapping[y],
                "amount": z * biosphere_scales[x],
                "flip": False,
                "uncertainty_type": 0,
            }
            for x, y, z in self.biosphere_iterator
        )

        dependents = [new_biosphere, main_biosphere]

        IOTableBackend(self.db_name).write_exchanges(
            technosphere, biosphere, dependents
        )
