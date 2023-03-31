from time import time
from ..extractors import Ecospold1LCIAExtractor
from .base_lcia import LCIAImporter


class Ecospold1LCIAImporter(LCIAImporter):
    """
    Importer for Ecospold1 LCIA format.

    Attributes
    ----------
    format : str
        The format of the LCIA data, which is "Ecospold1 LCIA".
    data : dict
        The LCIA data extracted from the Ecospold1 LCIA file.

    """
    format = "Ecospold1 LCIA"

    def __init__(self, filepath, biosphere=None):
        """
        Initialize the Ecospold1LCIAImporter instance.

        Parameters
        ----------
        filepath : str
            Path to the Ecospold1 LCIA file.
        biosphere : bw2data.BiosphereDatabase, optional
            Biosphere database to use. If None, the default biosphere database will be used.

        """
        super(Ecospold1LCIAImporter, self).__init__(filepath, biosphere)
        start = time()
        self.data = Ecospold1LCIAExtractor.extract(filepath)
        print(
            "Extracted {} methods in {:.2f} seconds".format(
                len(self.data), time() - start
            )
        )
