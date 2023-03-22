import json
import os

from bw2data.utils import recursive_str_to_unicode
from lxml import objectify

from ..strategies import (
    drop_unspecified_subcategories,
    ensure_categories_are_tuples,
    normalize_units,
)
from .base_lci import LCIImporter


EMISSIONS_CATEGORIES = {
    "air": "emission",
    "soil": "emission",
    "water": "emission",
}


class Ecospold2BiosphereImporter(LCIImporter):
    """
    Import elementary flows from ecoinvent xml format.

    Attributes
    ----------
    format : str
        Format of the data: "Ecoinvent XML".
    db_name : str
        Name of the database.
    data : list
        Extracted data from the xml file.
    strategies : list
        List of functions to apply to the extracted data.
    
    See Also
    --------
    https://github.com/brightway-lca/brightway2-io/tree/main/bw2io/strategies

    """

    format = "Ecoinvent XML"

    def __init__(self, name="biosphere3", version="3.9"):
        """
        Initialize the importer.

        Parameters
        ----------
        name : str, optional
            Name of the database, by default "biosphere3".
        version : str, optional
            Version of the database, by default "3.9".
        """
        self.db_name = name
        self.data = self.extract(version)
        self.strategies = [
            normalize_units,
            drop_unspecified_subcategories,
            ensure_categories_are_tuples,
        ]

    def extract(self, version):
        """
        Extract elementary flows from the xml file.

        Parameters
        ----------
        version : str
            Version of the database.

        Returns
        -------
        list
            Extracted data from the xml file.
        """

        def extract_flow_data(o):
            ds = {
                "categories": (
                    o.compartment.compartment.text,
                    o.compartment.subcompartment.text,
                ),
                "code": o.get("id"),
                "CAS number": o.get("casNumber"),
                "name": o.name.text,
                "database": self.db_name,
                "exchanges": [],
                "unit": o.unitName.text,
            }
            ds["type"] = EMISSIONS_CATEGORIES.get(
                ds["categories"][0], ds["categories"][0]
            )
            return ds

        lci_dirpath = os.path.join(os.path.dirname(__file__), "..", "data", "lci")

        fp = os.path.join(lci_dirpath, f"ecoinvent elementary flows {version}.xml")
        root = objectify.parse(open(fp, encoding="utf-8")).getroot()
        flow_data = recursive_str_to_unicode(
            [extract_flow_data(ds) for ds in root.iterchildren()]
        )

        # previous = os.path.join(lci_dirpath, "previous elementary flows.json")
        # return flow_data + json.load(open(previous))
        return flow_data
