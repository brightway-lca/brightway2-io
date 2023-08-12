import functools
from time import time

from bw2data import Database, config

from ..errors import MultiprocessingError
from ..extractors import Ecospold1DataExtractor
from ..strategies import (
    assign_only_product_as_production,
    clean_integer_codes,
    delete_integer_codes,
    drop_unspecified_subcategories,
    es1_allocate_multioutput,
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    normalize_units,
    set_code_by_activity_hash,
    strip_biosphere_exc_locations,
    update_ecoinvent_locations,
)
from .base_lci import LCIImporter


class SingleOutputEcospold1Importer(LCIImporter):
    """
    Import and process single-output datasets in the ecospold 1 format.

    Notes
    -----
    Applies the following strategies:
    1. If only one exchange is a production exchange, that is the reference product.
    2. Delete (unreliable) integer codes from extracted data.
    3. Drop ``unspecified`` subcategories from biosphere flows.
    4. Normalize biosphere flow categories to ecoinvent 3.1 standard.
    5. Normalize biosphere flow names to ecoinvent 3.1 standard.
    6. Remove locations from biosphere exchanges.
    7. Create a ``code`` from the activity hash of the dataset.
    8. Link biosphere exchanges to the default biosphere database.
    9. Link internal technosphere exchanges.

    """

    format = u"Ecospold1"

    def __init__(
        self, filepath, db_name, use_mp=True, extractor=Ecospold1DataExtractor
    ):
        """
        Parameters
        ----------
        filepath: str or Path
            File or directory path.
        db_name: str
            Name of database to create.
        use_mp: bool, optional
            Whether to use multiprocessing. Default is True.
        extractor: Type[Ecospold1DataExtractor], optional
            Data extractor to use. Default is Ecospold1DataExtractor.
        """
        self.strategies = [
            normalize_units,
            assign_only_product_as_production,
            clean_integer_codes,
            drop_unspecified_subcategories,
            normalize_biosphere_categories,
            normalize_biosphere_names,
            strip_biosphere_exc_locations,
            update_ecoinvent_locations,
            functools.partial(set_code_by_activity_hash, overwrite=True),
            functools.partial(
                link_iterable_by_fields,
                other=Database(config.biosphere),
                kind="biosphere",
            ),
            functools.partial(
                link_technosphere_by_activity_hash,
                fields=("name", "categories", "unit", "location"),
            ),
        ]
        self.db_name = db_name
        start = time()
        try:
            self.data = extractor.extract(filepath, db_name, use_mp=use_mp)
        except RuntimeError as e:
            raise MultiprocessingError(
                "Multiprocessing error; re-run using `use_mp=False`"
            ).with_traceback(e.__traceback__)
        print(
            u"Extracted {} datasets in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )

class NoIntegerCodesEcospold1Importer(SingleOutputEcospold1Importer):
    """
    An importer class that deletes integer codes from ecospold1 datasets.
    
    Parameters
    ----------
    SingleOutputEcospold1Importer : class
        The base importer class.
    
    Attributes
    ----------
    strategies : list
        A list of strategies that the importer applies to process the dataset.
    
    Returns
    -------
    None

    """
    def __init__(self, *args, **kwargs):
        """
        Initialize NoIntegerCodesEcospold1Importer.
        
        Parameters
        ----------
        *args : tuple
            Variable length argument list.
        **kwargs : dict
            Arbitrary keyword arguments.
        
        Returns
        -------
        None
        """
        super(NoIntegerCodesEcospold1Importer, self).__init__(*args, **kwargs)
        self.strategies.insert(0, delete_integer_codes)


class MultiOutputEcospold1Importer(SingleOutputEcospold1Importer):
    """
    Import and process multi-output datasets in the ecospold 1 format.

    Works the same as the single-output importer, but first allocates multioutput datasets.
    
    Attributes
    ----------
    strategies : list
        A list of strategies that the importer applies to process the dataset.
    
    Returns
    -------
    None
    
    """
    def __init__(self, *args, **kwargs):
        """
        Initialize MultiOutputEcospold1Importer.
        
        Parameters
        ----------
        *args : tuple
            Variable length argument list.
        **kwargs : dict
            Arbitrary keyword arguments.
        
        Returns
        -------
        None
        """
        self.strategies.insert(0, es1_allocate_multioutput)
        super(MultiOutputEcospold1Importer, self).__init__(*args, **kwargs)

