import re
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import bw2data as bd
import ecoinvent_interface as ei
import openpyxl
from ecoinvent_interface.core import SYSTEM_MODELS
from ecoinvent_interface.string_distance import damerau_levenshtein

from .extractors import ExcelExtractor
from .importers import (
    EcoinventLCIAImporter,
    Ecospold2BiosphereImporter,
    SingleOutputEcospold2Importer,
)


def get_excel_sheet_names(file_path: Path) -> list[str]:
    """Read XML metadata file instead of using openpyxl, which loads the whole workbook.

    From https://stackoverflow.com/questions/12250024/how-to-obtain-sheet-names-from-xls-files-without-loading-the-whole-file.
    """
    sheets = []
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        xml = zip_ref.read("xl/workbook.xml").decode("utf-8")
        for s_tag in re.findall("<sheet [^>]*", xml):
            sheets.append(re.search('name="[^"]*', s_tag).group(0)[6:])
    return sheets


def header_dict(array: list) -> list[dict]:
    return [
        {header.lower(): value for header, value in zip(array[0], row)}
        for row in array[1:]
        if any(row)
    ]


def drop_unspecified(a: str, b: str, c: str) -> tuple:
    if c.lower() == "unspecified":
        return (a, b)
    else:
        return (a, b, c)


def pick_a_unit_label_already(obj: dict) -> str:
    candidates = ("indicator unit", "unit", "unitname", "impact score unit")
    for candidate in candidates:
        if candidate in obj:
            return candidate
    raise KeyError("Can't find suitable column label for LCIA units")


def import_ecoinvent_release(
    version: str,
    system_model: str,
    username: str | None = None,
    password: str | None = None,
    lci: bool = True,
    lcia: bool = True,
    biosphere_name: str | None = None,
    use_existing_biosphere: bool = False,
    importer_signal: Any = None,
) -> None:
    """
    Import an ecoinvent LCI and/or LCIA release.

    Uses [ecoinvent_interface](https://github.com/brightway-lca/ecoinvent_interface).
    Auth credentials are optional as they can be set externally (see the
    `ecoinvent_interface` documentation), and such permanent storage is highly
    recommended.

    **DO NOT** run `bw2setup` before using this function - it isn't needed and
    will cause broken results.

    System model strings follow the ecoinvent unofficial API. They can be given
    in a short or long form. The short forms:

    * cutoff
    * consequential
    * apos
    * EN15804

    And the long forms:

    * Allocation cut-off by classification
    * Substitution, consequential, long-term
    * Allocation at the Point of Substitution
    * Allocation, cut-off, EN15804"

    Parameters
    ----------
    version
        The ecoinvent release version as a string, e.g. '3.9.1'
    system_model
        The system model as a string in short or long form, e.g. 'apos' or
        'Allocation cut-off by classification'
    username
        ecoinvent username
    password
        ecoinvent password
    lci
        Flag on whether to import the inventory database
    lcia
        Flag on whether to import the LCIA impact categories. The biosphere
        database must exist if `lci` is `False`
    biosphere_name
        Name of database to store biosphere flows. They will be stored in the
        main LCI database if not specified.
    use_existing_biosphere
        Flag on whether to create a new biosphere database or use an existing one
    importer_signal
        Used by the Activity Browser to provide feedback during the import

    Examples
    --------

    Get ecoinvent 3.9.1 cutoff in a new project (**without** running `bw2setup` first):

    >>> import bw2data as bd
    >>> import bw2io as bi
    >>> bd.projects.set_current("some new project")
    >>> bi.import_ecoinvent_release(
    ...     version="3.9.1",
    ...     system_model="cutoff",
    ...     username="XXX",
    ...     password="XXX"",
    ...     )
    >>> bd.databases
    Databases dictionary with 2 object(s):
        ecoinvent-3.9.1-biosphere
        ecoinvent-3.9.1-cutoff
    >>> len(bd.methods)
    762

    Add ecoinvent 3.9.1 apos to the same project:

    >>> bi.import_ecoinvent_release(
    ...     version="3.9.1",
    ...     system_model="apos",
    ...     username="XXX",
    ...     password="XXX"",
    ...     use_existing_biosphere=True
    ...     )
    >>> bd.databases
    Databases dictionary with 3 object(s):
        ecoinvent-3.9.1-apos
        ecoinvent-3.9.1-biosphere
        ecoinvent-3.9.1-cutoff

    Create a new database but use `biosphere3` for the biosphere database name
    and don't add LCIA methods:

    >>> bd.projects.set_current("some other project")
    >>> bi.import_ecoinvent_release(
    ...     version="3.9.1",
    ...     system_model="cutoff",
    ...     username="XXX",
    ...     password="XXX",
    ...     biosphere_name="biosphere3",
    ...     lcia=False
    ...     )
    >>> bd.databases
    Databases dictionary with 2 object(s):
        biosphere3
        ecoinvent-3.9.1-cutoff
    >>> len(bd.methods)
    0

    """
    from . import create_core_migrations, migrations

    if not len(migrations):
        create_core_migrations()

    if username is None and password is None:
        settings = ei.Settings()
    else:
        settings = ei.Settings(username=username, password=password)
    if not settings.username or not settings.password:
        raise ValueError("Can't determine ecoinvent username or password")

    release = ei.EcoinventRelease(settings)
    if not version in release.list_versions():
        raise ValueError(f"Invalid version {version}")

    if system_model in SYSTEM_MODELS:
        system_model = SYSTEM_MODELS[system_model]
    if not system_model in release.list_system_models(version):
        raise ValueError(f"Invalid system model {system_model}")

    if biosphere_name is None:
        biosphere_name = f"ecoinvent-{version}-biosphere"
    if lci:
        lci_path = release.get_release(
            version=version,
            system_model=system_model,
            release_type=ei.ReleaseType.ecospold,
        )

        db_name = f"ecoinvent-{version}-{system_model}"
        if db_name in bd.databases:
            raise ValueError(f"Database {db_name} already exists")

        if use_existing_biosphere:
            if biosphere_name not in bd.databases:
                raise ValueError(f"Biosphere database {biosphere_name} doesn't exist")
            elif not len(bd.Database(biosphere_name)):
                raise ValueError(f"Biosphere database {biosphere_name} is empty")
        else:
            if biosphere_name in bd.databases:
                raise ValueError(f"Biosphere database {biosphere_name} already exists")

            eb = Ecospold2BiosphereImporter(
                name=biosphere_name,
                filepath=lci_path / "MasterData" / "ElementaryExchanges.xml",
            )
            eb.apply_strategies()
            if not eb.all_linked:
                raise ValueError(
                    f"Can't ingest biosphere database {biosphere_name} - unlinked flows."
                )
            eb.write_database(overwrite=False)
        bd.preferences["biosphere_database"] = biosphere_name

        soup = SingleOutputEcospold2Importer(
            dirpath=lci_path / "datasets",
            db_name=db_name,
            biosphere_database_name=biosphere_name,
            signal=importer_signal,
        )
        soup.apply_strategies()
        if not soup.all_linked:
            raise ValueError(
                f"Can't ingest inventory database {db_name} - unlinked flows."
            )
        soup.write_database()

    if lcia:
        if biosphere_name is None:
            biosphere_name = bd.config.biosphere
        if biosphere_name not in bd.databases or not len(bd.Database(biosphere_name)):
            raise ValueError(
                f"Can't find populated biosphere flow database {biosphere_name}"
            )

        lcia_file = ei.get_excel_lcia_file_for_version(release=release, version=version)
        sheet_names = get_excel_sheet_names(lcia_file)

        if "units" in sheet_names:
            units_sheetname = "units"
        elif "Indicators" in sheet_names:
            units_sheetname = "Indicators"
        else:
            raise ValueError(
                f"Can't find worksheet for impact category units in {sheet_names}"
            )

        if "CFs" not in sheet_names:
            raise ValueError(
                f"Can't find worksheet for characterization factors; expected `CFs`, found {sheet_names}"
            )

        data = dict(ExcelExtractor.extract(lcia_file))
        units = header_dict(data[units_sheetname])

        cfs = header_dict(data["CFs"])

        CF_COLUMN_LABELS = {
            "3.4": "cf 3.4",
            "3.5": "cf 3.5",
            "3.6": "cf 3.6",
        }
        cf_col_label = CF_COLUMN_LABELS.get(version, "cf")
        units_col_label = pick_a_unit_label_already(units[0])
        units_mapping = {
            (row["method"], row["category"], row["indicator"]): row[units_col_label]
            for row in units
        }

        biosphere_mapping = {}
        for flow in bd.Database(biosphere_name):
            biosphere_mapping[(flow["name"],) + tuple(flow["categories"])] = flow.id
            if flow["name"].startswith("[Deleted]"):
                biosphere_mapping[
                    (flow["name"].replace("[Deleted]", ""),) + tuple(flow["categories"])
                ] = flow.id

        lcia_data_as_dict = defaultdict(list)

        unmatched = set()
        substituted = set()

        for row in cfs:
            impact_category = (row["method"], row["category"], row["indicator"])
            if row[cf_col_label] is None:
                continue
            try:
                lcia_data_as_dict[impact_category].append(
                    (
                        biosphere_mapping[
                            drop_unspecified(
                                row["name"], row["compartment"], row["subcompartment"]
                            )
                        ],
                        float(row[cf_col_label]),
                    )
                )
            except KeyError:
                # How is this possible? We are matching ecoinvent data against
                # ecoinvent data from the same release! And yet it moves...
                category = (
                    (row["compartment"], row["subcompartment"])
                    if row["subcompartment"].lower() != "unspecified"
                    else (row["compartment"],)
                )
                same_context = {
                    k[0]: v for k, v in biosphere_mapping.items() if k[1:] == category
                }
                candidates = sorted(
                    [
                        (damerau_levenshtein(name, row["name"]), name)
                        for name in same_context
                    ]
                )
                if (
                    candidates[0][0] < 3
                    and candidates[0][0] != candidates[1][0]
                    and candidates[0][1][0].lower() == row["name"][0].lower()
                ):
                    new_name = candidates[0][1]
                    pair = (new_name, row["name"])
                    if pair not in substituted:
                        print(f"Substituting {new_name} for {row['name']}")
                        substituted.add(pair)
                    lcia_data_as_dict[impact_category].append(
                        (
                            same_context[new_name],
                            float(row[cf_col_label]),
                        )
                    )
                else:
                    if row["name"] not in unmatched:
                        print(
                            "Skipping unmatched flow {}:({}, {})".format(
                                row["name"], row["compartment"], row["subcompartment"]
                            )
                        )
                        unmatched.add(row["name"])

        for key in lcia_data_as_dict:
            method = bd.Method(key)
            method.register(
                unit=units_mapping.get(key, "Unknown"),
                filepath=str(lcia_file),
                ecoinvent_version=version,
                database=biosphere_name,
            )
            method.write(lcia_data_as_dict[key])
