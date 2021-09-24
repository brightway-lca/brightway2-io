import numpy as np
import pandas as pd
import scipy.sparse
from bw2data.backends.iotable import IOTableBackend
from bw2data import Database, databases, config
from bw_processing.constants import INDICES_DTYPE

# from ..strategies.exiobase import (
#     rename_exiobase_co2_eq_flows,
#     normalize_units,
#     add_stam_labels,
#     get_exiobase_biosphere_correspondence,
#     add_biosphere_ids,
#     remove_numeric_codes,
#     add_product_ids,
# )
from pathlib import Path

try:
    from mrio_common_metadata.conversion.exiobase_3_hybrid_io import (
        Loader as ExiobaseLoader,
    )
    from bw_migrations import load_and_clean_exiobase_3_ecoinvent_36_migration
except ImportError:
    raise ImportError("This class requires Python version 3.")


class Exiobase3HybridImporter(object):
    format = "Exiobase 3.3.17 hybrid mrio_common_metadata tidy datapackage"

    def __init__(self, dirpath, db_name="EXIOBASE 3.3.17 hybrid"):

        self.strategies = []
        self.dirpath = Path(dirpath)
        self.db_name = db_name
        self.technosphere_indices = None
        self.biosphere_indices = None

        exio = ExiobaseLoader(self.dirpath)

        # load principle production data (dataframe)
        principle_production = exio.load_principal_production()
        # convert into dataframe with compatible format
        df_pp = self.convert_principle_production(principle_production)

        # load technosphere data (sparse matrix)
        A_technosphere = exio.load_technosphere(as_dataframe=False)
        # convert into dict with compatible format
        d_technosphere = self.convert_technosphere(A_technosphere, df_pp)

        # load biosphere data (dataframe)
        df_biosphere = exio.load_biosphere()
        # convert into dataframe with compatible format
        d_biosphere, exio_biosph_name, ei_biosph_name = self.convert_biosphere(
            df_biosphere
        )

        # apply additional patches
        # self.apply_strategies()

        # other databases, which the technosphere depends on
        dependents = [exio_biosph_name, ei_biosph_name]

        # create and write package
        IOTableBackend(self.db_name).write_exchanges(
            d_technosphere, d_biosphere, dependents
        )

        return

    def convert_principle_production(self, data):

        # rename and drop columns for bw compatibility
        rename_cols = {
            "sector name": "name",
            "product": "reference product",
            "principal production": "production volume",
        }
        drop_cols = [
            "sector code 1",
            "sector code 2",
            "product code 1",
            "product code 2",
        ]
        df = data.reset_index().rename(columns=rename_cols).drop(columns=drop_cols)
        df["exchanges"] = [[]] * len(df)

        # create keys: (db name, location|name)
        df["id"] = [
            (self.db_name, x["location"] + "|" + x["name"]) for i, x in df.iterrows()
        ]
        df = df.set_index("id")
        return df

    def convert_technosphere(self, A_tech, df_princ_prod):

        # get global index for each activity
        self.technosphere_indices = self.get_global_technosphere_indices(df_princ_prod)

        # pack technosphere data in a bw_processing-compatible format
        technosphere_data = dict(
            # map local rows and columns of sparse matrix to global indices
            indices_array=np.array(
                [
                    (self.technosphere_indices[row], self.technosphere_indices[col])
                    for row, col in zip(*A_tech.nonzero())
                ],
                dtype=INDICES_DTYPE,
            ),
            # flow amounts
            data_array=A_tech.data,
            # flip sign of flow: false for diagonal entries, true otherwise
            flip_array=np.array(
                [row != col for row, col in zip(*A_tech.nonzero())],
                dtype=bool
            ),
        )

        return technosphere_data

    def make_exiobase_biosphere(self, df, name=None, use_cols=None):
        # which columns to write to database
        if use_cols is None:
            use_cols = ["name", "compartment", "unit", "exchanges"]
        # default name for new biosphere
        if name is None:
            name = self.db_name + " biosphere"
        # copy df
        data = df.reset_index().copy()
        # add exchanges column
        data["exchanges"] = [[]] * len(data)
        # set key as index
        data["key"] = data["name"].apply(lambda n: (name, n))
        data = data.set_index("key")
        # filter columns and convert to dict
        data = data[use_cols].to_dict(orient="index")
        # create database
        db = Database(name)
        if name not in databases:
            db.register(format="EXIOBASE 3 New Biosphere", filepath=str(self.dirpath))
        db.write(data)
        return name

    def convert_biosphere(self, data):

        # check if technosphere was already created
        if self.technosphere_indices is None:
            raise Exception("Error: Must convert technosphere first.")

        # load exiobase biosphere <-> ecoinvent biosphere correspondence file
        ei_biosph_name = config.biosphere
        biosphere_mapper = load_and_clean_exiobase_3_ecoinvent_36_migration(
            ei_biosph_name
        )
        # drop all columns except conversion factor and ecoinvent biosphere index
        biosphere_mapper = biosphere_mapper[["factor", "biosphere index"]]

        # reorder matrix index to align with mapper
        data.index = data.index.swaplevel("compartment", "unit")
        # fill empty compartments with dummy value to facilitate joining
        data.index = pd.MultiIndex.from_frame(
            data.index.to_frame().fillna({"compartment": "undef"})
        )

        # add 'units' column to mapper index
        biosphere_mapper.index.names = ["name", "compartment"]
        biosphere_mapper.index = biosphere_mapper.index.join(data.index)

        # get unmapped flows
        df_unmapped = biosphere_mapper.reindex(data.index)
        df_unmapped = df_unmapped[df_unmapped["biosphere index"].isna()]
        df_unmapped["factor"] = [[1]] * len(df_unmapped)

        # create new biosphere and write unmapped flows
        exio_biosph_name = self.make_exiobase_biosphere(df_unmapped)
        # global indices of newly created exiobase biosphere flows
        indices = pd.DataFrame(Database(exio_biosph_name)).set_index(
            ["name", "compartment", "unit"]
        )["id"]
        df_unmapped = df_unmapped.fillna({"biosphere index": indices})
        # convert indices into list for consistency
        df_unmapped.loc[:, "biosphere index"] = df_unmapped["biosphere index"].apply(
            lambda x: [x]
        )

        # join ecoinvent biosphere flows and exiobase biosphere flows into one mapper
        # make sure order is identical to biosphere matrix
        biosphere_mapper = biosphere_mapper.append(df_unmapped).loc[data.index]
        # replace multilevel-index columns by single-level to prevent errors when joining
        data.columns = data.columns.to_list()
        # add biosphere indices to matrix and explode to get one-to-many mappings
        data = (
            data.join(biosphere_mapper["biosphere index"])
            .explode("biosphere index")
            .set_index("biosphere index")
        )
        self.biosphere_indices = data.index.values
        # multiply by conversion factors
        factors = (
            biosphere_mapper.explode(["factor", "biosphere index"])
            .set_index("biosphere index")["factor"]
            .astype("float")
        )
        data = (data.T * factors).T
        # convert to sparse matrix
        A_bio = scipy.sparse.coo_matrix(data.values)

        # create bw_processing compatible dict
        biosphere = dict(
            indices_array=np.array(
                [
                    (self.biosphere_indices[row], self.technosphere_indices[col])
                    for row, col in zip(*A_bio.nonzero())
                ],
                dtype=INDICES_DTYPE,
            ),
            # flow amounts
            data_array=A_bio.data,
            # flip signs yes or no
            flip_array=np.array([False] * len(A_bio.data), dtype=bool),
        )

        return biosphere, exio_biosph_name, ei_biosph_name

    def get_global_technosphere_indices(self, df):
        mapper = pd.Series(index=df.index.map(lambda i: str(i)))
        # write activities to database to generate global indices
        self.write_activities_as_database(df)
        # read global indices from database and write into mapper
        for o in Database(self.db_name):
            mapper[str(o.key)] = o.id
        # make sure all activities are mapped
        assert not mapper.isna().any()
        # return np.array containing global indices
        return mapper.values

    # def apply_strategies(self, biosphere=None):
    #     normalize_units(self.products)
    #     normalize_units(self.biosphere_correspondence, "exiobase unit")
    #     rename_exiobase_co2_eq_flows(self.biosphere_correspondence)
    #     remove_numeric_codes(self.products)
    #     add_stam_labels(self.products)

    def write_activities_as_database(self, df):
        db = IOTableBackend(self.db_name)
        data = df.to_dict(orient="index")
        if self.db_name not in databases:
            db.register(format="EXIOBASE 3", filepath=str(self.dirpath))
        db.write(data)
