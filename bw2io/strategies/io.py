from pathlib import Path
import itertools
import os
import typing

import numpy as np
import pandas as pd
import scipy
import bw2data as bd


def iob3_mapping() -> dict:

    # recent ecoinvent
    biosphere_mapping = {
        "co2_air": "349b29d1-3e58-4c66-98b9-9d1a076efd2e",
        "ch4_air": "0795345f-c7ae-410c-ad25-1845784c75f5",
    }
    return biosphere_mapping


def add_product_ids(products, db_name):

    mapping = {o["code"]: o.id for o in bd.Database(db_name)}

    for product in products:
        product["id"] = mapping["{}|{}".format(product["code"], product["location"])]

    return products


## to tidy the dataframes


def tidy_tables(
    A: pd.DataFrame, B: pd.DataFrame, dirpath: typing.Union[str, os.PathLike]
):
    """It converts the IO table and the B extension table into a series of gzip
    files stored in dirpath. These are later used to be imported into Brightway.

    It assumes that the A and B tables are in sparse data format, that
    their columns are aligned, (e.g. identical), and that the A table index and 
    columns are sorted so the diagonal contains the reference products.

    Parameters
    ----------
    A : pd.DataFrame
        IO table. What in brigthway is called "technosphere matrix", in LCA is
        sometimes called technology matrix and in IO technical coefficient matrix
        (althoght it does need to be normalized). If the final demand is part of
        the model it should be integrated in the matrix. 
    B : pd.DataFrame
        intervention matrix (aka biosphere matrix or satellite matrix)
    dirpath : typing.Union[str, os.PathLike]
        path where the files will be stored.
    """

    _tidy_iotable(A, dirpath)
    _tidy_extension_table(B, dirpath)


def _tidy_iotable(io_table: pd.DataFrame, path):
    """_tidy_iotable creates csvs with a tidy version of the IO table given in
    a typical matrix format as pandas dataframe.
    Args:
        io_table (pd.DataFrame): _description_
        path (_type_): _description_
    """
    # TODO: add convertion step to sparse

    iot_coo = io_table.sparse.to_coo()
    # NOTE: pandas sparse can introduced undersired zeros during concat
    iot_coo.eliminate_zeros()

    # read the data
    index_table = _index_table_df(io_table)
    prod_table = _prod_df(iot_coo)
    tech_table = _tech_table_df(iot_coo)

    # load into csvs
    Path(path).mkdir(parents=True, exist_ok=True)

    # table with
    index_table.to_csv(path / "index_table_iot.gzip", index=None, compression="gzip")

    prod_table.to_csv(path / "product_value_table.gzip", index=None, compression="gzip")

    tech_table.to_csv(
        path / "technosphere_value_table.gzip", index=None, compression="gzip"
    )


def _tidy_extension_table(B: pd.DataFrame, path):

    index_table_extensions = _index_table_extensions(B)

    b_coo = B.sparse.to_coo()
    b_coo.eliminate_zeros()
    b_tidy_values = _bio_table_df(b_coo)

    index_table_extensions.to_csv(
        path / "index_table_extensions.gzip", index=None, compression="gzip"
    )

    b_tidy_values.to_csv(
        path / "extensions_value_table.gzip", index=None, compression="gzip"
    )


def _prod_df(iot_coo) -> pd.DataFrame:
    """_prod_df stuff in the diagonal. make sure this makes sense

    Args:
        iot_coo (scipy.sparse._coo.coo_matrix): _description_

    Returns:
        pd.DataFrame: _description_
    """
    prod_df = pd.DataFrame(
        [
            (row, col, value)
            for row, col, value in zip(iot_coo.row, iot_coo.col, iot_coo.data)
            if col == row
        ]
    )

    prod_df.columns = ["row", "col", "amount"]

    n_zeros = (prod_df["amount"].map(lambda x: np.isclose(0, x)) == True).sum()
    assert n_zeros == 0, "zeros in the diagonal"

    return prod_df


def _tech_table_df(iot_coo) -> pd.DataFrame:
    """_prod_df _summary_

    Args:
        iot_coo (scipy.sparse._coo.coo_matrix): _description_

    Returns:
        pd.DataFrame: _description_
    """
    # only the values of the off-diagonal
    tech_df = pd.DataFrame(
        [
            (row, col, value)
            for row, col, value in zip(iot_coo.row, iot_coo.col, iot_coo.data)
            if col != row
        ]
    )

    if tech_df.empty:
        # case of a terminated version, there will be no use
        tech_df.columns = []
    else:
        tech_df.columns = ["row", "col", "amount"]

    return tech_df


def _index_table_df(iot: pd.DataFrame) -> pd.DataFrame:
    """having an MR iot table in matrix format as input it generates a table
    with the indices of rows and cols

    Parameters
    ----------
    iot : pd.DataFrame
        _description_

    Returns
    -------
    pd.DataFrame
        _description_
    """
    v = [
        (col_region, col_code, row_region, row_code)
        for (col_region, col_code), (row_region, row_code) in zip(
            iot.columns, iot.index
        )
    ]
    df = pd.DataFrame(v, columns=["col_region", "col_code", "row_region", "row_code"])
    df.index.name = "index"
    return df


def _index_table_extensions(extension_table: pd.DataFrame) -> pd.DataFrame:
    """generates a dataframe with tidy information about the index and header of
    the extension matrix

    Parameters
    ----------
    extension_table : pd.DataFrame
        _description_

    Returns
    -------
    pd.DataFrame
        _description_
    """
    df = pd.Series({i: name for i, name in enumerate(extension_table.index)}).to_frame(
        "row_code"
    )
    df.index.name = "index"
    return df


def _bio_table_df(b_coo) -> pd.DataFrame:

    b_tidy = pd.DataFrame(
        [(row, col, value) for row, col, value in zip(b_coo.row, b_coo.col, b_coo.data)]
    )
    b_tidy.columns = ["row", "col", "amount"]

    return b_tidy


def create_coo(value_table: pd.DataFrame):

    coo_matrix = scipy.sparse.coo_matrix(
        (value_table.amount, (value_table.row, value_table.col))
    )

    return coo_matrix


def create_dataframe(coo_matrix, row_dict: dict, col_dict: dict):

    df = pd.DataFrame.sparse.from_spmatrix(coo_matrix)
    df = df.rename(row_dict, axis=0).rename(col_dict, axis=1)
    df.columns = pd.MultiIndex.from_tuples(df.columns)
    df.index = pd.MultiIndex.from_tuples(df.index)

    return df
