__version__ = (0, 1)

from .bw2package import BW2Package, download_biosphere, download_methods
from .export import (
    DatabaseToGEXF, DatabaseSelectionToGEXF, keyword_to_gephi_graph,
    lci_matrices_to_excel,
    lci_matrices_to_matlab,
)
from .backup import backup_data_directory
from .imprt import (
    Ecospold1LCIAImporter,
    MultiOutputEcospold1Importer,
    SimaProCSVImporter,
    SimaProLCIACSVImporter,
    SingleOutputEcospold1Importer,
    SingleOutputEcospold2Importer,
    Ecospold2BiosphereImporter,
)
from .units import normalize_units
from .unlinked_data import unlinked_data, UnlinkedData
from .utils import activity_hash, es2_activity_hash, load_json_data_file


def create_biosphere3():
    from .imprt import Ecospold2BiosphereImporter
    bi = Ecospold2BiosphereImporter()
    bi.write_database()


def bw2setup():
    create_biosphere3()
