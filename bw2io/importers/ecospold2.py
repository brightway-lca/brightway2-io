from typing import Any
from functools import partial
from pathlib import Path
from time import time

from bw2data import Database, config

from ..errors import MultiprocessingError
from ..extractors import Ecospold2DataExtractor
from ..strategies import (
    add_cpc_classification_from_single_reference_product,
    assign_single_product_as_activity,
    convert_activity_parameters_to_list,
    create_composite_code,
    delete_exchanges_missing_activity,
    delete_ghost_exchanges,
    delete_none_synonyms,
    drop_temporary_outdated_biosphere_flows,
    drop_unspecified_subcategories,
    es2_assign_only_product_with_amount_as_reference_product,
    fix_ecoinvent_flows_pre35,
    fix_unreasonably_high_lognormal_uncertainties,
    link_biosphere_by_flow_uuid,
    link_internal_technosphere_by_composite_code,
    normalize_units,
    remove_uncertainty_from_negative_loss_exchanges,
    remove_unnamed_parameters,
    remove_zero_amount_coproducts,
    remove_zero_amount_inputs_with_no_activity,
    reparametrize_lognormal_to_agree_with_static_amount,
    set_lognormal_loc_value,
    update_ecoinvent_locations,
    update_social_flows_in_older_consequential,
)
from .base_lci import LCIImporter


class SingleOutputEcospold2Importer(LCIImporter):

    """
    Class for importing single-output ecospold2 format LCI databases.

    Raises
    ------
    MultiprocessingError
        If an error occurs during multiprocessing.
    
    """

    format = "Ecospold2"

    def __init__(
        self,
        dirpath: str,
        db_name: str,
        biosphere_database_name: str | None = None,
        extractor: Any=Ecospold2DataExtractor,
        use_mp: bool=True,
        signal: Any=None,
        reparametrize_lognormals: bool=False,
    ):

        """
        Initializes the SingleOutputEcospold2Importer class instance.

        Parameters
        ----------
        dirpath : str
            Path to the directory containing the ecospold2 file.
        db_name : str
            Name of the LCI database.
        biosphere_database_name : str | None
            Name of biosphere database to link to. Uses `config.biosphere` if not provided.
        extractor : class
            Class for extracting data from the ecospold2 file, by default Ecospold2DataExtractor.
        use_mp : bool
            Flag to indicate whether to use multiprocessing, by default True.
        signal : object
            Object to indicate the status of the import process, by default None.
        reparametrize_lognormals: bool
            Flag to indicate if lognormal distributions for exchanges should be reparametrized
            such that the mean value of the resulting distribution meets the amount
            defined for the exchange.
        """
        
        self.dirpath = dirpath

        if not Path(dirpath).is_dir():
            raise ValueError(f"`dirpath` value was not a directory: {dirpath}")

        self.db_name = db_name
        self.signal = signal
        self.strategies = [
            normalize_units,
            update_ecoinvent_locations,
            remove_zero_amount_coproducts,
            remove_zero_amount_inputs_with_no_activity,
            remove_unnamed_parameters,
            es2_assign_only_product_with_amount_as_reference_product,
            assign_single_product_as_activity,
            create_composite_code,
            drop_unspecified_subcategories,
            fix_ecoinvent_flows_pre35,
            drop_temporary_outdated_biosphere_flows,
            partial(link_biosphere_by_flow_uuid, biosphere=biosphere_database_name or config.biosphere),
            link_internal_technosphere_by_composite_code,
            delete_exchanges_missing_activity,
            delete_ghost_exchanges,
            remove_uncertainty_from_negative_loss_exchanges,
            fix_unreasonably_high_lognormal_uncertainties,
            convert_activity_parameters_to_list,
            add_cpc_classification_from_single_reference_product,
            delete_none_synonyms,
            partial(update_social_flows_in_older_consequential, biosphere_db=Database(biosphere_database_name or config.biosphere)),
        ]

        if reparametrize_lognormals:
            self.strategies.append(reparametrize_lognormal_to_agree_with_static_amount)
        else:
            self.strategies.append(set_lognormal_loc_value)

        start = time()
        try:
            self.data = extractor.extract(dirpath, db_name, use_mp=use_mp)
        except RuntimeError as e:
            raise MultiprocessingError(
                "Multiprocessing error; re-run using `use_mp=False`"
            ).with_traceback(e.__traceback__)
        print(
            u"Extracted {} datasets in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )
