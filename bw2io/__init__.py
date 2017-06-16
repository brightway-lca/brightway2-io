# -*- coding: utf-8 -*-
__all__ = [
    'activity_hash',
    'add_ecoinvent_33_biosphere_flows',
    'backup_data_directory',
    'backup_project_directory',
    'BW2Package',
    'bw2setup',
    'create_core_migrations',
    'create_default_biosphere3',
    'create_default_lcia_methods',
    'CSVImporter',
    'DatabaseSelectionToGEXF',
    'DatabaseToGEXF',
    'Ecospold1LCIAImporter',
    'es2_activity_hash',
    'ExcelImporter',
    'get_csv_example_filepath',
    'get_xlsx_example_filepath',
    'lci_matrices_to_excel',
    'lci_matrices_to_matlab',
    'load_json_data_file',
    'Migration',
    'migrations',
    'MultiOutputEcospold1Importer',
    'normalize_units',
    'restore_project_directory',
    'SimaProCSVImporter',
    'SimaProLCIACSVImporter',
    'SingleOutputEcospold1Importer',
    'SingleOutputEcospold2Importer',
    'unlinked_data',
    'UnlinkedData',
]

__version__ = (0, 5, 10)


from .package import BW2Package
from .export import (
    DatabaseToGEXF, DatabaseSelectionToGEXF, keyword_to_gephi_graph,
    lci_matrices_to_excel,
    lci_matrices_to_matlab,
)
from .backup import (
    backup_data_directory,
    backup_project_directory,
    restore_project_directory,
)
from .data import (
    add_ecoinvent_33_biosphere_flows,
    get_csv_example_filepath,
    get_xlsx_example_filepath,
)
from .migrations import migrations, Migration, create_core_migrations
from .importers import (
    CSVImporter,
    Ecospold1LCIAImporter,
    ExcelImporter,
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


def create_default_biosphere3(overwrite=False):
    from .importers import Ecospold2BiosphereImporter
    eb = Ecospold2BiosphereImporter()
    eb.apply_strategies()
    eb.write_database(overwrite=overwrite)

def create_default_lcia_methods(overwrite=False):
    from .importers import EcoinventLCIAImporter
    ei = EcoinventLCIAImporter()
    ei.apply_strategies()
    ei.write_methods(overwrite=overwrite)

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
