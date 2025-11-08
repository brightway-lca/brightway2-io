import json
from pathlib import Path
import csv
import gzip
from typing import Union

import pandas as pd


def _get_metadata(path):

    with open(path / "io_metadata.json", "r") as fp:
        meta = json.load(fp)

    return meta


def _get_products(path: Union[str, Path]) -> list:
    """_get_products returns a list of dictionaries with the following keys:
    code, location, unit, production volume

    Args:
        path (_type_): path to folder with product_value_table.csv and index.csv
        table

    Returns:
        list: _description_
    """

    prod_volumes = pd.read_csv(
        Path(path) / "product_value_table.gzip", compression="gzip"
    )
    col_row_labels = pd.read_csv(
        Path(path) / "index_table_hiot.gzip", compression="gzip"
    )

    # dict with act key, prod key for reference products
    # TODO this assumes the ref prod is in the diagonal.. I am not sure
    ref_prod_dict = col_row_labels.set_index("col_code").row_code.to_dict()
    ref_prod_dict_rev = {v: k for k, v in ref_prod_dict.items()}

    # TODO: remove because it is duplicated
    # metadata:
    with open(Path(path) / "io_metadata.json", "r") as fp:
        meta = json.load(fp)

    products = []
    # TODO: use something faster than pandas for this.
    for i, prod_vol_row in prod_volumes.iterrows():
        pv = prod_vol_row.amount

        location = col_row_labels.loc[i, "col_region"]
        act_code = col_row_labels.loc[i, "col_code"]
        # assuming rp in diagonal
        prod_code = col_row_labels.loc[i, "row_code"]

        unit = meta[prod_code]["unit"]
        ref_prod_name = meta[prod_code]["name"]

        d = {
            "production volume": pv,
            "location": location,
            "code": act_code,
            "unit": unit,
            "reference product": ref_prod_name,
        }
        products.append(d)

    return products


def gzipped_csv_line_generator(file_path):

    with gzip.open(file_path, "rt", newline="") as gzfile:
        csv_reader = csv.reader(gzfile)

        # Read the header
        header = next(csv_reader, None)

        # Yield the remaining lines
        for row in csv_reader:
            yield {k: v for k, v in zip(header, row)}


class IOHybridExtractor(object):

    @classmethod
    def get_products(cls, dirpath):

        return _get_products(dirpath)

    @classmethod
    def _get_hiot_index_dict(cls, dirpath: Union[str, Path]) -> dict:
        """_summary_

        Parameters
        ----------
        dirpath : Union[str,Path]
            _description_

        Returns
        -------
        dict
            position in the hiot table as key and unique key of the activity
            as value
        """
        table = pd.read_csv(Path(dirpath) / "index_table_hiot.gzip", compression="gzip")

        col_code_dict = (table.col_code + "|" + table.col_region).to_dict()

        return col_code_dict

    @classmethod
    def _get_extensions_index_dict(cls, dirpath: Union[str, Path]) -> dict:

        index_extensions = pd.read_csv(
            Path(dirpath) / "index_table_extensions.gzip", compression="gzip"
        )

        index_extensions_dict = index_extensions.row_code.to_dict()

        return index_extensions_dict

    @classmethod
    def _product_iterator(cls, path):

        table_path = path / "product_value_table.gzip"

        generator = gzipped_csv_line_generator(table_path)

        for dict in generator:

            yield dict

    @classmethod
    def _technosphere_iterator(cls, path):

        table_path = Path(path) / "technosphere_value_table.gzip"
        generator = gzipped_csv_line_generator(table_path)

        for dict in generator:

            yield dict

    @classmethod
    def _biosphere_iterator(cls, path):

        table_path = Path(path) / "extensions_value_table.gzip"

        generator = gzipped_csv_line_generator(table_path)

        for dict in generator:

            yield dict

    @classmethod
    def get_metadata(cls, path):

        meta = _get_metadata(path)

        return meta
