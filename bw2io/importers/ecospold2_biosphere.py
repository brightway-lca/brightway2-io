import json
import os
from pathlib import Path
from typing import Optional

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

    def __init__(
        self,
        name: str = "biosphere3",
        version: str = "3.9",
        filepath: Optional[Path] = None,
    ):
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
        self.data = self.extract(version, filepath)
        self.strategies = [
            normalize_units,
            drop_unspecified_subcategories,
            ensure_categories_are_tuples,
        ]

    def extract(self, version: Optional[str] = None, filepath: Optional[Path] = None):
        """
        Extract elementary flows from the xml file.

        Parameters
        ----------
        version
            Version of the database if using default data.
        filepath
            File path of user-specified data file

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
                "synonyms": [
                    elem.text.strip()
                    for elem in o.iterchildren()
                    if elem.tag == "{http://www.EcoInvent.org/EcoSpold02}synonym"
                    and elem.text  # 3.7.1 has blank elements
                    and elem.text.strip()
                ],
                "name": o.name.text,
                "database": self.db_name,
                "exchanges": [],
                "unit": o.unitName.text,
            }
            ds["type"] = EMISSIONS_CATEGORIES.get(
                ds["categories"][0], ds["categories"][0]
            )
            return ds

        if not filepath:
            filepath = (
                Path(__file__).parent.parent.resolve()
                / "data"
                / "lci"
                / f"ecoinvent elementary flows {version}.xml"
            )

        root = objectify.parse(open(filepath, encoding="utf-8")).getroot()
        flow_data = [extract_flow_data(ds) for ds in root.iterchildren()]
        return flow_data
