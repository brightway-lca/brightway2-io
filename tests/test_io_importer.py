...
import json
from pathlib import Path
from typing import Tuple
import pandas as pd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw2io.strategies.io import tidy_tables
from bw2io.importers.io import IOImporter
from bw2io.extractors.io import IOHybridExtractor
import tempfile
import bw2data as bd


@pytest.fixture
def a_and_b_matrices() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """_summary_

    Returns:
        tuple: (A,B), the first element has an example of a technosphere
        matrix, the second an example of a biosphere matrix
    """

    pet_hiot = pd.DataFrame(
        [[1, -2], [0, 1]],  # experiment setting 1 prod to 0
        index=pd.MultiIndex.from_tuples([("DK", "prod1"), ("DK", "prod2")]),
        columns=pd.MultiIndex.from_tuples([("DK", "act1"), ("DK", "act2")]),
    )

    pet_hiot = pet_hiot.astype(pd.SparseDtype("float", 0))

    B = pd.DataFrame(
        [[1, 3], [1, 2], [0, 1], [4, 0]],
        index=pd.Index(
            ["co2_air", "ch4_air", "co2_accelerated_air", "land_occupation"]
        ),
        columns=pd.MultiIndex.from_tuples([("DK", "act1"), ("DK", "act2")]),
    )

    B = B.astype(pd.SparseDtype("float", 0))

    fd = pd.DataFrame.from_dict(
        {
            ("DK", "Household"): {("DK", "prod1"): -11, ("DK", "prod2"): -3},
            ("DK", "Government"): {("DK", "prod1"): -8, ("DK", "prod2"): -4},
            ("DK", "Capital"): {("DK", "prod1"): -4, ("DK", "prod2"): -2},
        }
    )

    Bfd = pd.DataFrame(
        [
            [1, 3],
        ],
        index=pd.Index(
            [
                "co2_air",
            ]
        ),
        columns=pd.MultiIndex.from_tuples([("DK", "Household"), ("DK", "Government")]),
    ).astype(pd.SparseDtype("float", 0))

    pfd = pd.DataFrame((np.eye(fd.shape[1])), index=fd.columns, columns=fd.columns)
    fd_total = pd.concat([fd, pfd])
    fd_total = fd_total.astype(pd.SparseDtype("float", 0))

    extended_hiot = pd.concat([pet_hiot, fd_total], axis=1).fillna(0)
    extended_B = pd.concat([B, Bfd], axis=1).fillna(0)

    return (extended_hiot, extended_B)


@pytest.fixture
def metadata() -> dict:

    metadata_dict = {
        "prod1": {"unit": "kg", "name": "product 1"},
        "prod2": {"unit": "kg", "name": "product 2"},
        "Household": {"unit": "unit", "name": "the household"},
        "Government": {"unit": "unit", "name": "the government"},
        "Capital": {"unit": "unit", "name": "capital investments"},
        "co2_air": {
            "unit": "ton",  # not standard units
            "name": "carbon dioxide",
            "compartment": ("air",),
        },
        "ch4_air": {"unit": "kg", "name": "methane", "compartment": ("air",)},
        "co2_accelerated_air": {
            "unit": "kg",  # additional biosphere flow
            "name": "carbon dioxide accelerated",
            "compartment": ("air",),
        },
        "land_occupation": {
            "unit": "hectare * year",  # non standard composite unit
            "name": "land occupation",
            "compartment": ("natural resource", "land"),
        },
    }

    return metadata_dict


def test_tidy_tables(a_and_b_matrices):
    """after tidying the tables a number of files should be created"""

    (A, B) = a_and_b_matrices

    with tempfile.TemporaryDirectory() as temp_dir:

        tidy_tables(A, B, temp_dir)

        assert (
            Path(temp_dir) / "extensions_value_table.gzip"
        ).is_file(), "missing B value table"
        assert (
            Path(temp_dir) / "index_table_technosphere.gzip"
        ).is_file(), "missing A index table"
        assert (
            Path(temp_dir) / "product_value_table.gzip"
        ).is_file(), "missing A supply  values"
        assert (
            Path(temp_dir) / "technosphere_value_table.gzip"
        ).is_file(), "missing A use values"
        assert (
            Path(temp_dir) / "index_table_extensions.gzip"
        ).is_file(), "missing B index table"


@pytest.fixture
def tidy_folder(tmp_path, a_and_b_matrices, metadata: dict):

    (A, B) = a_and_b_matrices

    tidy_tables(A, B, dirpath=tmp_path)

    # add metadata as a json
    with open(tmp_path / "io_metadata.json", "w") as fp:
        json.dump(metadata, fp, indent=4)

    return tmp_path


def test_tidy_folder(tidy_folder):

    assert (
        tidy_folder / "extensions_value_table.gzip"
    ).is_file(), "missing B value table"
    assert (tidy_folder / "io_metadata.json").is_file()


def test_IO_extractor(tidy_folder):

    dirpath = tidy_folder

    metadata = IOHybridExtractor.get_metadata(dirpath)

    products = IOHybridExtractor.get_products(dirpath)
    technosphere_iterator = IOHybridExtractor._technosphere_iterator(dirpath)
    biosphere_iterator = IOHybridExtractor._biosphere_iterator(dirpath)
    production_iterator = IOHybridExtractor._product_iterator(dirpath)
    index_iot_dict = IOHybridExtractor._get_iot_index_dict(dirpath)
    index_extn_dict = IOHybridExtractor._get_extensions_index_dict(dirpath)


def test_io_importer(tidy_folder):

    biosphere_mapping = {
        "co2_air": "349b29d1-3e58-4c66-98b9-9d1a076efd2e",
        "ch4_air": "0795345f-c7ae-410c-ad25-1845784c75f5",
        "land_occupation": "c7cb5880-4219-4051-9357-10fdd08c6f2b",
    }

    importer = IOImporter(
        dirpath=tidy_folder, db_name="io_test", b3mapping=biosphere_mapping
    )

    importer.apply_strategies()

    with pytest.raises(ValueError):
        # if biosphere mapping is not provided it raises an error
        importer = IOImporter(dirpath=tidy_folder, db_name="io_test")


