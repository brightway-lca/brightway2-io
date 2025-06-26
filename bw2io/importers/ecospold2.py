from functools import partial
from pathlib import Path
from time import time
from typing import Any, Optional

from bw2data import Database, config
from bw2data.logs import stdout_feedback_logger

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
    separate_processes_from_products,
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
        biosphere_database_name: Optional[str] = None,
        extractor: Any = Ecospold2DataExtractor,
        use_mp: bool = True,
        signal: Any = None,
        reparametrize_lognormals: bool = False,
        add_product_information: bool = True,
        separate_products: bool = False,
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
        add_product_information: bool
            Add the `productInformation` text from `MasterData/IntermediateExchanges.xml` to
            `product_information`.
        separate_products: bool
            Import processes and products as separate nodes in the supply chain graph.
        """

        self.dirpath = Path(dirpath)

        if not self.dirpath.is_dir():
            raise ValueError(f"`dirpath` value was not a directory: {self.dirpath}")

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
            partial(
                link_biosphere_by_flow_uuid,
                biosphere=biosphere_database_name or config.biosphere,
            ),
            link_internal_technosphere_by_composite_code,
            delete_exchanges_missing_activity,
            delete_ghost_exchanges,
            remove_uncertainty_from_negative_loss_exchanges,
            fix_unreasonably_high_lognormal_uncertainties,
            convert_activity_parameters_to_list,
            add_cpc_classification_from_single_reference_product,
            delete_none_synonyms,
            partial(
                update_social_flows_in_older_consequential,
                biosphere_db=Database(biosphere_database_name or config.biosphere),
            ),
        ]

        if reparametrize_lognormals:
            self.strategies.append(reparametrize_lognormal_to_agree_with_static_amount)
        else:
            self.strategies.append(set_lognormal_loc_value)

        if separate_products:
            self.strategies.append(separate_processes_from_products)

        start = time()
        try:
            self.data = extractor.extract(self.dirpath, db_name, use_mp=use_mp)
        except RuntimeError as e:
            raise MultiprocessingError(
                "Multiprocessing error; re-run using `use_mp=False`"
            ).with_traceback(e.__traceback__)
        stdout_feedback_logger.info(
            "Extracted {} datasets in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )
        if add_product_information:
            tm_dirpath = self.dirpath.parent / "MasterData"
            if not tm_dirpath.is_dir():
                stdout_feedback_logger.warning(
                    "Skipping product information as `MasterData` directory not found"
                )
            else:
                technosphere_metadata = {
                    obj["id"]: obj["product_information"]
                    for obj in extractor.extract_technosphere_metadata(tm_dirpath)
                }
                for ds in self.data:
                    ds["product_information"] = technosphere_metadata[
                        ds["filename"].replace(".spold", "").split("_")[1]
                    ]
