__version__ = (0, 2, "dev2")

from .package import BW2Package, download_biosphere, download_methods
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

from bw2data import config, databases
config.metadata.extend([
    migrations,
    unlinked_data,
])


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
    if "biosphere3" in databases:
        print("Biosphere database already present!!! No setup is needed")
        return
    print("Creating default biosphere\n")
    create_default_biosphere3()
    print("Creating default LCIA methods\n")
    create_default_lcia_methods()
    print("Creating core data migrations\n")
    create_core_migrations()
