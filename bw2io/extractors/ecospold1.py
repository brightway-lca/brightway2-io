import math
import multiprocessing
import os
from io import StringIO
from pathlib import Path
from typing import Any, Optional, Union

import numpy as np
import pyecospold
from lxml import etree
from stats_arrays.distributions import (
    LognormalUncertainty,
    NormalUncertainty,
    TriangularUncertainty,
    UndefinedUncertainty,
    UniformUncertainty,
)
from tqdm import tqdm


def robust_text(root: etree.ElementBase, attribute: str) -> Optional[str]:
    """Just because the spec says it must be there doesn't mean it will be."""
    try:
        return getattr(root, attribute).text
    except AttributeError:
        return None


def robust_nested_attribute(root: etree.ElementBase, attr1: str, attr2: str) -> Any:
    """Try to get nested attribute, and fail gracefully."""
    try:
        first_level = getattr(root, attr1)
        if first_level is None:
            return None
        return getattr(first_level, attr2)
    except AttributeError:
        return None


class Ecospold1DataExtractor:
    @classmethod
    def extract(
        cls, path: Union[str, Path, StringIO], db_name: str, use_mp: bool = True
    ):
        """
        Extract data from ecospold1 files.

        Parameters
        ----------
        path : str
            Path to the directory containing the ecospold1 files or path to a single file.
        db_name : str
            Name of the database.
        use_mp : bool, optional
            If True, uses multiprocessing to parallelize extraction of data from multiple files, by default True.

        Returns
        -------
        list
            List of dictionaries containing data from the ecospold1 files.

        """
        data = []
        if os.path.isdir(path):
            filelist = [
                os.path.join(path, filename)
                for filename in os.listdir(path)
                if filename[-4:].lower() == ".xml"
                # Skip SimaPro-specific flow list
                and filename != "ElementaryFlows.xml"
            ]
        else:
            filelist = [path]

        if not filelist:
            raise OSError("Provided path doesn't appear to have any XML files")

        if use_mp:
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                print("Extracting XML data from {} datasets".format(len(filelist)))
                results = [
                    pool.apply_async(
                        Ecospold1DataExtractor.process_file, args=(x, db_name)
                    )
                    for x in filelist
                ]
                data = [x for p in results for x in p.get() if x]

        else:
            data = []

            for index, filepath in enumerate(tqdm(filelist)):
                for x in cls.process_file(filepath, db_name):
                    if x:
                        data.append(x)

        return data

    @classmethod
    def process_file(cls, filepath: Union[str, Path, StringIO], db_name: str):
        """
        Process a single ecospold1 file.

        Parameters
        ----------
        filepath : str
            Path to the ecospold1 file.
        db_name : str
            Name of the database.

        Returns
        -------
        list
            List of dictionaries containing data from the ecospold1 file.

        """
        root = pyecospold.parse_file_v1(filepath)
        data = []

        for dataset in root.datasets:
            if dataset.tag == "comment":
                continue
            data.append(cls.process_dataset(dataset, filepath, db_name))
        return data

    @classmethod
    def process_dataset(
        cls,
        dataset: pyecospold.model_v1.Dataset,
        filename: Union[str, Path, StringIO],
        db_name: str,
    ):
        MI = dataset.metaInformation
        PI = MI.processInformation
        RF = PI.referenceFunction
        MV = MI.modellingAndValidation

        comments = {
            "generalComment": RF.generalComment,
            "includedProcesses": RF.includedProcesses,
            "location": "Location: " + PI.geography.text,
            "technology": "Technology: " + PI.technology.text,
            "timePeriod": "Time period: " + PI.timePeriod.text,
            "productionVolume": "Production volume: "
            + (
                robust_nested_attribute(MV, "representativeness", "productionVolume")
                or ""
            ),
            "sampling": "Sampling: "
            + (
                robust_nested_attribute(MV, "representativeness", "samplingProcedure")
                or ""
            ),
            "extrapolations": "Extrapolations: "
            + (
                robust_nested_attribute(MV, "representativeness", "extrapolations")
                or ""
            ),
            "uncertaintyAdjustments": "Uncertainty adjustments: "
            + (
                robust_nested_attribute(
                    MV, "representativeness", "uncertaintyAdjustments"
                )
                or ""
            ),
        }

        def get_authors():
            AI = MI.administrativeInformation

            PERSON_FIELDS = [
                ("address", "address"),
                ("company", "companyCode"),
                ("country", "countryCode"),
                ("email", "email"),
                ("name", "name"),
            ]

            people = {
                person.number: {a: getattr(person, b, "") for a, b in PERSON_FIELDS}
                for person in AI.persons
            }

            data = {
                "data_entry": people[AI.dataEntryBy.person],
            }

            # Good, good, let the hate flow through you
            unique_people = {}
            for person in people.values():
                if not any(person == other for other in unique_people.values()):
                    unique_people[len(unique_people) + 1] = person

            for k, v in unique_people.items():
                # Because we added the *same* dict to `data_entry`, this
                # also gets the correct identifier there.
                v["identifier"] = k

            data["people"] = list(unique_people.values())

            # We don't extract the `dataGeneratorAndPublication` tag because
            # it is insane; there is only one but we have multiple publications,
            # and implementing software puts in garbage anyway

            return data

        data = {
            "tags": [
                ("ecoSpold01datasetRelatesToProduct", RF.datasetRelatesToProduct),
                ("ecoSpold01infrastructureProcess", RF.infrastructureProcess),
                ("ecoSpold01infrastructureIncluded", RF.infrastructureIncluded),
                ("ecoSpold01localName", RF.localName),
                ("ecoSpold01localCategory", RF.localCategory),
                ("ecoSpold01localSubCategory", RF.localSubCategory),
                ("ecoSpold01category", RF.category),
                ("ecoSpold01subCategory", RF.subCategory),
                ("ecoSpold01includedProcesses", RF.includedProcesses),
                (
                    "ecoSpold01dataValidForEntirePeriod",
                    PI.timePeriod.dataValidForEntirePeriod,
                ),
                # Get string representation instead of converting to native
                # date type
                ("ecoSpold01endDate", PI.timePeriod.endDate.strftime("%Y-%m-%d")),
                ("ecoSpold01startDate", PI.timePeriod.startDate.strftime("%Y-%m-%d")),
                ("ecoSpold01type", PI.dataSetInformation.type),
                (
                    "ecoSpold01impactAssessmentResult",
                    PI.dataSetInformation.impactAssessmentResult,
                ),
                ("ecoSpold01version", PI.dataSetInformation.version),
                (
                    "ecoSpold01internalVersion",
                    PI.dataSetInformation.internalVersion,
                ),
                ("ecoSpold01timestamp", PI.dataSetInformation.timestamp.isoformat()),
                ("ecoSpold01languageCode", PI.dataSetInformation.languageCode),
                (
                    "ecoSpold01localLanguageCode",
                    PI.dataSetInformation.localLanguageCode,
                ),
                ("ecoSpold01energyValues", PI.dataSetInformation.energyValues),
            ],
            "references": [
                {
                    "identifier": source.number,
                    "type": source.sourceTypeStr,
                    # additional authors supposed to be split by comma, but comma
                    # also used in first/last names, so can split names.
                    # Just add as long string
                    "authors": [source.firstAuthor, source.additionalAuthors],
                    "year": source.year,
                    "title": source.title,
                    "pages": source.pageNumbers,
                    "editors": source.nameOfEditors,
                    "anthology": source.titleOfAnthology,
                    "place_of_publication": source.placeOfPublications,
                    "publisher": source.publisher,
                    "journal": source.journal,
                    "volume": source.volumeNo,
                    "issue": source.issueNo,
                    "text": source.text,
                }
                for source in MV.sources
            ],
            "categories": [RF.get("category"), RF.get("subCategory")],
            "code": int(dataset.get("number")),
            "comment": "\n".join(text for text in comments.values() if text),
            "comments": comments,
            "authors": get_authors(),
            "database": db_name,
            "exchanges": cls.process_exchanges(dataset),
            "filename": (
                Path(filename).name
                if not isinstance(filename, StringIO)
                else "StringIO"
            ),
            "location": PI.geography.location,
            "name": RF.name.strip(),
            "unit": RF.unit,
            "type": "process",
        }

        allocation_exchanges = [
            exc for exc in data["exchanges"] if exc.get("reference")
        ]

        if allocation_exchanges:
            data["allocations"] = allocation_exchanges
            data["exchanges"] = [exc for exc in data["exchanges"] if exc.get("type")]

        return data

    @classmethod
    def process_exchanges(cls, dataset):
        data = []
        # Skip definitional exchange - we assume this already
        for exc in dataset.flowData.exchanges:
            data.append(cls.process_exchange(exc, dataset))
        for exc in dataset.flowData.allocations:
            data.append(cls.process_allocation(exc, dataset))
        return data

    @classmethod
    def process_allocation(cls, exc, dataset):
        return {
            "reference": int(exc.get("referenceToCoProduct")),
            "fraction": float(exc.get("fraction")),
            "exchanges": [
                int(c.text) for c in exc.iterchildren() if c.tag != "comment"
            ],
        }

    @classmethod
    def process_exchange(cls, exc, dataset):
        """Process exchange.

        Input groups are:

            1. Materials/fuels
            2. Electricity/Heat
            3. Services
            4. FromNature
            5. FromTechnosphere

        Output groups are:

            0. Reference product
            1. Include avoided product system
            2. Allocated byproduct
            3. Waste to treatment
            4. ToNature

        A single-output process will have one output group 0; A MO process will have multiple output group 2s. Output groups 1 and 3 are not used in ecoinvent.

        """

        if exc.groupsStr[0] in (
            "ReferenceProduct",
            "Allocated by product",
            "WasteToTreatment",
        ):
            kind = "production"
        elif exc.groupsStr[0] == "Include avoided product system":
            kind = "substitution"
        elif exc.groupsStr[0] == "ToNature":
            kind = "biosphere"
        elif exc.groupsStr[0] in (
            "Materials/Fuels",
            "Electricity/Heat",
            "Services",
            "FromTechnosphere",
        ):
            kind = "technosphere"
        elif exc.groupsStr[0] == "FromNature":
            kind = "biosphere"  # Resources
        else:
            raise ValueError("Can't understand exchange group {}".format(exc.groupsStr))

        data = {
            "code": int(exc.number or 0),
            "categories": (exc.get("category"), exc.get("subCategory")),
            "location": exc.location,
            "unit": exc.unit,
            "name": exc.name.strip(),
            "type": kind,
            "infrastructureProcess": exc.infrastructureProcess,
        }

        if exc.generalComment:
            data["comment"] = exc.generalComment
        if exc.CASNumber:
            data["CAS number"] = exc.CASNumber
        if exc.formula:
            data["chemical formula"] = exc.formula
        if exc.referenceToSource:
            data["source_reference"] = exc.referenceToSource
        if exc.pageNumbers:
            data["pages"] = exc.pageNumbers

        return cls.process_uncertainty_fields(exc, data)

    @classmethod
    def process_uncertainty_fields(cls, exc, data):
        uncertainty = int(exc.get("uncertaintyType", 0))

        def floatish(x):
            try:
                return float(x.strip())
            except:
                return np.NaN

        mean = floatish(exc.get("meanValue"))
        min_ = floatish(exc.get("minValue"))
        max_ = floatish(exc.get("maxValue"))
        sigma = floatish(exc.get("standardDeviation95"))

        if uncertainty == 1 and sigma in (0, 1):
            # Bad data
            uncertainty = 0

        if uncertainty == 1:
            # Lognormal
            data.update(
                {
                    "uncertainty type": LognormalUncertainty.id,
                    "amount": float(mean),
                    "loc": np.log(np.abs(mean)),
                    "scale": math.log(math.sqrt(float(sigma))),
                    "negative": mean < 0,
                }
            )
            if np.isnan(data["scale"]) or mean == 0:
                # Bad data
                data["uncertainty type"] = UndefinedUncertainty.id
                data["loc"] = data["amount"]
                del data["scale"]
        elif uncertainty == 2:
            # Normal
            data.update(
                {
                    "uncertainty type": NormalUncertainty.id,
                    "amount": float(mean),
                    "loc": float(mean),
                    "scale": float(sigma) / 2,
                }
            )
        elif uncertainty == 3:
            # Triangular
            data.update(
                {
                    "uncertainty type": TriangularUncertainty.id,
                    "minimum": float(min_),
                    "maximum": float(max_),
                }
            )
            # Sometimes this isn't included (though it SHOULD BE)
            if exc.get("mostLikelyValue"):
                mode = floatish(exc.get("mostLikelyValue"))
                data["amount"] = data["loc"] = mode
            else:
                data["amount"] = data["loc"] = float(mean)
        elif uncertainty == 4:
            # Uniform
            data.update(
                {
                    "uncertainty type": UniformUncertainty.id,
                    "amount": float(mean),
                    "minimum": float(min_),
                    "maximum": float(max_),
                }
            )
        else:
            # None
            data.update(
                {
                    "uncertainty type": UndefinedUncertainty.id,
                    "amount": float(mean),
                    "loc": float(mean),
                }
            )
        return data
