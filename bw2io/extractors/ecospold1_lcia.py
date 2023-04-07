import os
import sys

import pyprind
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify

try:
    import psutil
    monitor = True
except ImportError:
    monitor = False


def _to_unicode(data):
    if sys.version_info < (3, 0):
        return recursive_str_to_unicode(data)
    else:
        return data


class Ecospold1LCIAExtractor(object):
    """
    Extract impact assessment methods and weightings data from ecospold XML format.
    
    Attributes: 
        None
    
    Methods: 
        extract: Extracts data from an ecospold XML file.
        parse_method: Parses the ecospold XML dataset to extract information.
        parse_cf: Parses an ecospold XML data element to extract characterization factor information.    
    """

    @classmethod
    def extract(cls, path):
        """
        Extracts ecospold XML file data.

        Parameters
        ----------
        path : str
            The path to the ecospold XML file or directory.

        Returns
        -------
        list
            A list of dictionaries with the extracted information.
        
        """
        if os.path.isdir(path):
            files = [
                os.path.join(path, name)
                for name in os.listdir(path)
                if name[-4:].lower() == ".xml"
            ]
        else:
            files = [path]

        pbar = pyprind.ProgBar(
            len(files), title="Extracting ecospold1 files:", monitor=monitor
        )

        methods_data = []

        for filepath in files:
            # Note that this is only used for the first root method found in
            # the file
            root = objectify.parse(open(filepath, encoding="utf-8")).getroot()
            for dataset in root.iterchildren():
                methods_data.append(_to_unicode(cls.parse_method(dataset, filepath)))
            pbar.update(item_id=filepath[:15])
        print(pbar)
        return methods_data

    @classmethod
    def parse_method(cls, ds, filepath):
        """
        Parse and extract information from an ecospold XML dataset.

        Parameters
        ----------
        ds : object
            The XML dataset.
        filepath : str
            The path to the XML file.

        Returns
        -------
        dict
            A dictionary of the information extracted from the ecospold XML dataset.
        
        """
        ref_func = ds.metaInformation.processInformation.referenceFunction
        return {
            "exchanges": [cls.parse_cf(o) for o in ds.flowData.iterchildren()],
            "description": ref_func.get("generalComment") or "",
            "filename": filepath,
            "name": (
                ref_func.get("category"),
                ref_func.get("subCategory"),
                ref_func.get("name"),
            ),
            "unit": ref_func.get("unit") or "",
        }

    @classmethod
    def parse_cf(cls, cf):
        """
        Parse a cf object and extract relevant data.

        Parameters
        ----------
        cf : dict
            A dictionary of cf data.

        Returns
        -------
        dict
            A dictionary of parsed cf data.
        
        Raises
        ------
        TypeError
            If 'cf' is not a dictionary.
        
        Notes
        -----
        This method expects 'cf' to contain the following keys:
        - meanValue (float): the amount
        - category (str): the category
        - subCategory (str, optional): the subcategory, if any
        - name (str): the name
        - unit (str): the unit of the amount

        If `subCategory` is not provided, it will default to `None`.   
        """
        data = {
            "amount": float(cf.get("meanValue")),
            "categories": (cf.get("category"), cf.get("subCategory") or None),
            "name": cf.get("name"),
            "unit": cf.get("unit"),
        }
        return data
