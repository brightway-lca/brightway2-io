import itertools
from bw2io.extractors.io import IOHybridExtractor
from bw2io.strategies.io import add_product_ids
from bw2io.strategies.generic import normalize_units
from bw2io.units import normalize_units as normalize_units_function
from bw2io.importers.base_lci import LCIImporter
from bw2data.backends.iotable import IOTableBackend
from bw2io.units import get_default_units_migration_data
import bw2data as bd
import pint


class IOImporter(LCIImporter):
    """_summary_

    Args:
        LCIImporter (_type_): _description_
    """
    def __init__(self, dirpath, db_name, b3mapping: dict = None):
        self.dirpath = dirpath
        self.db_name = db_name
        self.metadata = IOHybridExtractor.get_metadata(dirpath)
        self.products = IOHybridExtractor.get_products(dirpath)
        self.technosphere_iterator = IOHybridExtractor._technosphere_iterator(dirpath)
        self.biosphere_iterator = IOHybridExtractor._biosphere_iterator(dirpath)
        self.production_iterator = IOHybridExtractor._product_iterator(dirpath)
        self.index_iot_dict = IOHybridExtractor._get_iot_index_dict(dirpath)
        self.index_extn_dict = IOHybridExtractor._get_extensions_index_dict(dirpath)

        # TODO: find a more elegant solution. Perhaps inside apply_strategies
        if b3mapping is None:
            raise ValueError('mapping to biosphere database is missing')
        else:
            self.biosphere_correspondence = b3mapping
        self.ureg = pint.UnitRegistry()

    def apply_strategies(self):

        # only the SQL part with metadata
        self.products = normalize_units(self.products)

    def add_unlinked_flows_to_new_biosphere_database(self, biosphere_name=None):

        biosphere_name = biosphere_name or self.db_name + " biosphere"
        db = bd.Database(biosphere_name)

        needed_ef = {
            key for key, value in self.metadata.items() if "compartment" in value
        }

        # codes of the extra activities neeeded
        extra_needed = needed_ef.difference(self.biosphere_correspondence)

        data = {
            (biosphere_name, o): {
                "name": self.metadata[o]["name"],
                "unit": normalize_units_function(self.metadata[o]["unit"]),
                "categories": tuple(self.metadata[o]["compartment"]),
                "type": "emission",  # FIXME : allow other types such as natural resource or economic
                "exchanges": [],
            }
            for o in extra_needed
        }

        if biosphere_name not in bd.databases:
            db.register(format="IO New Biosphere", filepath=str(self.dirpath))
            db.write(data)
        else:
            print("no extra biosphere flows added")

        return biosphere_name

    # write the SQL part (metadata)
    def write_activities_as_database(self):
        db = IOTableBackend(self.db_name)

        data = {}
        for o in self.products:

            code = "{}|{}".format(o["code"], o["location"])
            key = (self.db_name, code)

            o_code_in_metadata = o["code"] in self.metadata

            try:
                name = self.metadata[o["code"]]["name"]
            except KeyError:
                # the code is not in the metadata
                name = o["code"]

            d = {}
            d.update(o)
            d["name"] = name
            d["code"] = code
            d["exchanges"] = []

            data[key] = d

        if self.db_name not in bd.databases:
            db.register(format="EXIOBASE 3", filepath=str(self.dirpath))

        db.write(data)

    def write_database(self, biosphere=None):

        new_biosphere = self.add_unlinked_flows_to_new_biosphere_database()

        if biosphere is None:
            main_biosphere = bd.config.biosphere
        else:
            main_biosphere = biosphere

        assert main_biosphere in bd.databases, "target biosphere db missing"
        # write the metadata
        self.write_activities_as_database()
        add_product_ids(self.products, self.db_name)

        # product unique code to id
        product_mapping = {
            "{}|{}".format(o["code"], o["location"]): o["id"] for o in self.products
        }

        # biosphere io code to biosphere 3 id
        biosphere_mapping = {
            ext_code: bd.get_id((main_biosphere, b3_code))
            for ext_code, b3_code in self.biosphere_correspondence.items()
        }

        extra_mapping = {ef["code"]: ef["id"] for ef in bd.Database(new_biosphere)}
        # add the ids of the extra elementary flows needed
        biosphere_mapping.update(extra_mapping)

        # io
        unit_conversion = {
            code: {
                "io_unit": normalize_units_function(self.metadata[code]["unit"]),
                "b3_unit": bd.get_node(id=_b3id)["unit"],
            }
            for code, _b3id in biosphere_mapping.items()
        }

        # FIXME: add solution for cases of composite units with weird b3 units
        replacer = {"square meter-year": "square meter * year"}

        d = {}
        for code, conversion in unit_conversion.items():

            c = {
                io_unit: replacer.get(b3_unit, b3_unit)
                for io_unit, b3_unit in conversion.items()
            }
            d[code] = c

        unit_conversion = d

        # only the ones that are different
        unit_conversion = {
            u: u_dict
            for u, u_dict in unit_conversion.items()
            if u_dict["io_unit"] != u_dict["b3_unit"]
        }

        multipliers = {
            bcode: self.ureg(units_dict["io_unit"]).to(units_dict["b3_unit"]).magnitude
            for bcode, units_dict in unit_conversion.items()
        }

        technosphere = itertools.chain(
            (
                {
                    "row": product_mapping[self.index_iot_dict[int(t["row"])]],
                    "col": product_mapping[self.index_iot_dict[int(t["col"])]],
                    "amount": t["amount"],
                    "flip": False,  # production flow
                    "uncertainty_type": 0,
                }
                for t in self.technosphere_iterator
            ),
            (
                {
                    "row": product_mapping[self.index_iot_dict[int(p["row"])]],
                    "col": product_mapping[self.index_iot_dict[int(p["col"])]],
                    "amount": p["amount"],
                    "flip": False,  # already negative in the IO table
                    "uncertainty_type": 0,
                }
                for p in self.production_iterator
            ),
        )
        # NOTE: this works only if the biosphere and iot table have the same
        # number of columns and the same order
        biosphere = (
            {
                "row": biosphere_mapping[self.index_extn_dict[int(b["row"])]],
                "col": product_mapping[self.index_iot_dict[int(b["col"])]],
                "amount": float(b["amount"])
                * multipliers.get(self.index_extn_dict[int(b["row"])], 1),
                "flip": False,
                "uncertainty_type": 0,
            }
            for b in self.biosphere_iterator
        )

        dependents = [main_biosphere, new_biosphere]

        IOTableBackend(self.db_name).write_exchanges(
            technosphere, biosphere, dependents
        )
