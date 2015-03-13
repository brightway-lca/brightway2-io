__version__ = (0, 1)

from .bw2package import BW2Package, download_biosphere, download_methods
from .export import (
    DatabaseToGEXF, DatabaseSelectionToGEXF, keyword_to_gephi_graph,
    lci_matrices_to_excel,
    lci_matrices_to_matlab,
)
from .backup import backup_data_directory
from .migrations import migrations, Migration, create_core_migrations
from .importers import (
    Ecospold1LCIAImporter,
    MultiOutputEcospold1Importer,
    SimaProCSVImporter,
    SimaProLCIACSVImporter,
    SingleOutputEcospold1Importer,
    SingleOutputEcospold2Importer,
)
from .units import normalize_units
from .unlinked_data import unlinked_data, UnlinkedData
from .utils import activity_hash, es2_activity_hash, load_json_data_file


def create_default_biosphere3():
    from .importers import Ecospold2BiosphereImporter
    eb = Ecospold2BiosphereImporter()
    eb.apply_strategies()
    eb.write_database()

def create_default_lcia_methods():
    from .importers import EcoinventLCIAImporter
    ei = EcoinventLCIAImporter()
    ei.apply_strategies()
    ei.write_methods()

def bw2setup():
    create_default_biosphere3()
    create_default_lcia_methods()
    create_core_migrations()
