__all__ = [
    "activity_hash",
    "add_ecoinvent_33_biosphere_flows",
    "add_ecoinvent_34_biosphere_flows",
    "add_ecoinvent_35_biosphere_flows",
    "add_ecoinvent_36_biosphere_flows",
    "add_ecoinvent_37_biosphere_flows",
    "add_ecoinvent_38_biosphere_flows",
    "add_ecoinvent_39_biosphere_flows",
    "add_example_database",
    "backup_data_directory",
    "backup_project_directory",
    "BW2Package",
    "bw2setup",
    "ChemIDPlus",
    "create_core_migrations",
    "create_default_biosphere3",
    "create_default_lcia_methods",
    "CSVImporter",
    "CSVLCIAImporter",
    "DatabaseSelectionToGEXF",
    "DatabaseToGEXF",
    "Ecospold1LCIAImporter",
    "es2_activity_hash",
    "ExcelImporter",
    "ExcelLCIAImporter",
    "Exiobase3MonetaryImporter",
    "exiobase_monetary",
    "get_csv_example_filepath",
    "get_xlsx_example_filepath",
    "install_project",
    "lci_matrices_to_excel",
    "lci_matrices_to_matlab",
    "load_json_data_file",
    "Migration",
    "migrations",
    "MultiOutputEcospold1Importer",
    "normalize_units",
    "restore_project_directory",
    "SimaProCSVImporter",
    "SimaProLCIACSVImporter",
    "SingleOutputEcospold1Importer",
    "SingleOutputEcospold2Importer",
    "useeio11",
    "unlinked_data",
    "UnlinkedData",
]

from .version import version as __version__

from .chemidplus import ChemIDPlus
from .package import BW2Package
from .export import (
    DatabaseToGEXF,
    DatabaseSelectionToGEXF,
    keyword_to_gephi_graph,
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
    add_ecoinvent_34_biosphere_flows,
    add_ecoinvent_35_biosphere_flows,
    add_ecoinvent_36_biosphere_flows,
    add_ecoinvent_37_biosphere_flows,
    add_ecoinvent_38_biosphere_flows,
    add_ecoinvent_39_biosphere_flows,
    add_example_database,
    get_csv_example_filepath,
    get_xlsx_example_filepath,
)
from .migrations import migrations, Migration, create_core_migrations
from .importers import (
    CSVImporter,
    CSVLCIAImporter,
    Ecospold1LCIAImporter,
    ExcelImporter,
    ExcelLCIAImporter,
    MultiOutputEcospold1Importer,
    SimaProCSVImporter,
    SimaProLCIACSVImporter,
    SingleOutputEcospold1Importer,
    SingleOutputEcospold2Importer,
    Exiobase3MonetaryImporter,
    Exiobase3HybridImporter,
)
from .units import normalize_units
from .unlinked_data import unlinked_data, UnlinkedData
from .utils import activity_hash, es2_activity_hash, load_json_data_file
from .remote import install_project

from bw2data import config, databases

config.metadata.extend(
    [
        migrations,
        unlinked_data,
    ]
)


def create_default_biosphere3(overwrite=False):
    from .importers import Ecospold2BiosphereImporter

    eb = Ecospold2BiosphereImporter()
    eb.apply_strategies()
    eb.write_database(overwrite=overwrite)


def create_default_lcia_methods(
    overwrite=False, rationalize_method_names=False, shortcut=True
):
    if shortcut:
        import zipfile
        import json
        from pathlib import Path
        from .importers.base_lcia import LCIAImporter

        fp = Path(__file__).parent.resolve() / "data" / "lcia" / "lcia_39_ecoinvent.zip"

        with zipfile.ZipFile(fp, mode="r") as archive:
            data = json.load(archive.open("data.json"))

        for method in data:
            method["name"] = tuple(method["name"])

        ei = LCIAImporter("lcia_39_ecoinvent.zip")
        ei.data = data
        ei.write_methods(overwrite=overwrite)
    else:
        from .importers import EcoinventLCIAImporter

        ei = EcoinventLCIAImporter()
        if rationalize_method_names:
            ei.add_rationalize_method_names_strategy()
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


def useeio11(name="USEEIO-1.1", collapse_products=False, prune=False):
    """"""
    URL = "https://www.lcacommons.gov/lca-collaboration/ws/public/download/json/repository_US_Environmental_Protection_Agency@USEEIO"

    if name in databases:
        print(f"{name} already present")
        return

    from .importers.json_ld import JSONLDImporter
    from .importers.json_ld_lcia import JSONLDLCIAImporter
    from .strategies import remove_useeio_products, remove_random_exchanges
    from .download_utils import download_with_progressbar
    from pathlib import Path
    import tempfile
    import zipfile

    with tempfile.TemporaryDirectory() as td:
        dp = Path(td)
        print("Downloading US EEIO 1.1")
        filepath = Path(download_with_progressbar(URL, dirpath=td))

        print("Unzipping file")
        with zipfile.ZipFile(filepath, "r") as zip_ref:
            zip_ref.extractall(td)

        filepath.unlink()

        print("Importing data")
        j = JSONLDImporter(dp, name)
        j.apply_strategies(no_warning=True)
        j.merge_biosphere_flows()
        if collapse_products:
            j.apply_strategy(remove_useeio_products)
        if prune:
            j.apply_strategy(remove_random_exchanges)
        assert j.all_linked
        j.write_database()

        l = JSONLDLCIAImporter(dp)
        l.apply_strategies()
        l.match_biosphere_by_id(name)
        assert l.all_linked
        l.write_methods()


def exiobase_monetary(
    version=(3, 8, 1),
    year=2017,
    products=False,
    name=None,
    ignore_small_balancing_corrections=True,
):
    from .download_utils import download_with_progressbar
    import tempfile
    from pathlib import Path

    mapping = {
        (3, 8, 2): {
            "url": "https://zenodo.org/record/5589597/files/IOT_{year}_{system}.zip?download=1",
            "products": True,
        },
        (3, 8, 1): {
            "url": "https://zenodo.org/record/4588235/files/IOT_{year}_{system}.zip?download=1",
            "products": True,
        },
        (3, 8): {
            "url": "https://zenodo.org/record/4277368/files/IOT_{year}_{system}.zip?download=1",
            "products": True,
        },
        (3, 7): {
            "url": "https://zenodo.org/record/3583071/files/IOT_{year}_{system}.zip?download=1",
            "products": False,
        },
    }

    if name is None:
        name = "EXIOBASE {} {} monetary".format(
            ".".join([str(x) for x in version]), year
        )

    if version not in mapping:
        raise ValueError("`version` must be one of {}".format(list(mapping)))
    if products and not mapping[version]["products"]:
        raise ValueError(f"product by product table not availabe for version {version}")

    with tempfile.TemporaryDirectory() as td:
        url = mapping[version]["url"].format(
            year=year, system="pxp" if products else "ixi"
        )
        filepath = download_with_progressbar(url, dirpath=Path(td))
        ex = Exiobase3MonetaryImporter(
            filepath,
            name,
            ignore_small_balancing_corrections=ignore_small_balancing_corrections,
        )
        ex.apply_strategies()
        ex.write_database()

    print(f"Created database {name}. Cleaned up temporary downloads.")
