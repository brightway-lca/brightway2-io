from bw2data.backends.iotable import IOTableBackend
from bw2data import Database
from ..units import UNITS_NORMALIZATION
from copy import deepcopy
from pathlib import Path
import itertools
import re

try:
    from bw_migrations.strategies import get_migration, modify_object
    import mrio_common_metadata
except ImportError:
    raise ImportError("This class requires Python version 3.")


class Exiobase33Importer(object):
    format = "Exiobase 3.3.17 hybrid mrio_common_metadata tidy datapackage"

    def __init__(self, dirpath, db_name="EXIOBASE 3.3.17 hybrid"):
        self.strategies = []
        self.dirpath = Path(dirpath)
        self.db_name = db_name

        activities = mrio_common_metadata.get_metadata_resource(
            self.dirpath, "activities"
        )
        products = mrio_common_metadata.get_metadata_resource(self.dirpath, "products")

        product_to_activities = {i["id"]: j["id"] for i, j in zip(products, activities)}

        def as_process(o):
            o["type"] = "process"
            o["format"] = self.format
            o["key"] = (self.db_name, o["id"])
            return o

        def as_product(o):
            o["type"] = "product"
            o["format"] = self.format
            o["unit"] = UNITS_NORMALIZATION.get(o["unit"], o["unit"])
            o["key"] = (self.db_name, o["id"])
            return o

        activities = [as_process(o) for o in activities]
        products = [as_product(o) for o in products]

        # Take units from products
        assert len(activities) == len(products)
        for a, p in zip(activities, products):
            a["unit"] = p["unit"]

        # Clean names like 'Collection, purification and distribution of water (41)'
        numeric_end = re.compile("\(\d\d\)$")

        def clean_name(name):
            suffix = numeric_end.findall(name)
            if suffix:
                name = name.replace(suffix[0], "")
            return name.strip()

        for activity in activities:
            activity["name"] = clean_name(activity["name"])

        self.datasets = {
            **{(self.db_name, o.pop("id")): o for o in activities},
            # Adding products doesn't work, at least not in current code
            # **{(self.db_name, o.pop('id')): o for o in products},
        }

        # Construct three iterators: production, biosphere, and inputs

        def production_iterator():
            for i, j, amount in mrio_common_metadata.get_numeric_data_iterator(
                self.dirpath, "production-exchanges"
            ):
                yield {
                    # 'input': (self.db_name, i['id']),
                    "input": (self.db_name, j["id"]),
                    "output": (self.db_name, j["id"]),
                    "type": "production",
                    "amount": amount or 1,
                }

        def biosphere_iterator():
            """This is a pain in the butt, as we need to translate from the exiobase world to the ecoinvent flow list. Along the way, we have to deal with:

            1. Multiple EXIOBASE flows map to one ecoinvent flow
            2. Single EXIOBASE flows map to multiple ecoinvent flow
            3. Unit conversions and other numeric disaggregations
            4. Other metadata mappings

            Our strategy, therefore, is to create a dictionary from the EXIOBASE world, namely from ``(name, compartment)`` to a list of ecoinvent biosphere flow keys and disaggregation factors:

            .. code-block:: python

                {('Lead ores', ''): [(('biosphere3', 'fbcb9c7a-eea7-4694-ba6c-568e01d28883'), 1000)]}

            To do this, we first migrate the EXIOBASE data to what ecoinvent expects, and then link with actual ecoinvent keys.

            We operate on the master list of EXIOBASE flows instead of the exchanges.

            """
            biosphere_mapping = {
                (flow["name"], tuple(flow["categories"])): ("biosphere3", flow["code"])
                for flow in Database("biosphere3")
            }
            migration_data = {
                tuple(x): y
                for x, y in get_migration("exiobase-3-ecoinvent-3.6")["data"]
            }

            extensions_dict = {
                (o["name"], o.get("compartment")): o
                for o in mrio_common_metadata.get_metadata_resource(
                    self.dirpath, "extensions"
                )
            }
            for dct in extensions_dict.values():
                dct["amount"] = 1
                dct["categories"] = dct.get("compartment") or None

            def as_list(obj):
                if isinstance(obj, list):
                    return obj
                else:
                    return [obj]

            def normalize_categories(dct):
                if isinstance(dct["categories"], str):
                    dct["categories"] = (dct["categories"],)
                else:
                    dct["categories"] = tuple(dct["categories"])
                return dct

            def match_ecoinvent(dct):
                key = (dct["name"], dct["categories"])
                try:
                    return (biosphere_mapping[key], dct["amount"])
                except KeyError:
                    return None

            extensions_dict = {
                k: [
                    normalize_categories(modify_object(deepcopy(v), disaggregated))
                    for disaggregated in as_list(
                        migration_data[(v["name"], v["categories"])]
                    )
                ]
                for k, v in extensions_dict.items()
                if (v["name"], v["categories"]) in migration_data
            }

            extensions_dict = {
                k: [match_ecoinvent(elem) for elem in v if match_ecoinvent(elem)]
                for k, v in extensions_dict.items()
            }

            for i, j, amount in mrio_common_metadata.get_numeric_data_iterator(
                self.dirpath, "extension-exchanges"
            ):

                for key, scale in extensions_dict.get(
                    (i["name"], i.get("compartment")), []
                ):
                    yield {
                        "input": key,
                        "output": (self.db_name, j["id"]),
                        "type": "biosphere",
                        "amount": amount * scale,
                    }

        def technosphere_iterator():
            for i, j, amount in mrio_common_metadata.get_numeric_data_iterator(
                self.dirpath, "hiot"
            ):
                yield {
                    "input": (self.db_name, product_to_activities[i["id"]]),
                    "output": (self.db_name, j["id"]),
                    "type": "technosphere",
                    "amount": amount,
                }

        self.exchanges = itertools.chain(
            production_iterator(), biosphere_iterator(), technosphere_iterator()
        )

    def write_database(self):
        mrio = IOTableBackend(self.db_name)
        mrio.write(self.datasets, self.exchanges)
