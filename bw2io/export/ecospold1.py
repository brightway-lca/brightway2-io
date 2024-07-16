from datetime import datetime
from os import times
from pathlib import Path
from typing import Dict, Union

import numpy as np
from lxml import etree
from stats_arrays.distributions import (
    LognormalUncertainty,
    NormalUncertainty,
    NoUncertainty,
    TriangularUncertainty,
    UndefinedUncertainty,
    UniformUncertainty,
)

from .. import __version__ as version

attr_qname = etree.QName("http://www.w3.org/2001/XMLSchema-instance", "schemaLocation")
nsmap = {
    None: "http://www.EcoInvent.org/EcoSpold01",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

if isinstance(version, tuple):
    version = ".".join([str(x) for x in version])


def bool_to_text(b: Union[bool, str]) -> str:
    if b in (True, "yes", "Yes", "true", "True"):
        return "true"
    elif b in (False, None, "", "False", "false", "No", "no"):
        return "false"
    else:
        raise ValueError(f"Can't convert {b} to boolean string")


def stripper(obj: str, prefix: str) -> str:
    if obj.startswith(prefix):
        return obj[len(prefix) :]
    else:
        return obj


def pretty_number(val: float) -> str:
    if 1e-2 < abs(val) < 1e2:
        return np.format_float_positional(val, precision=6, trim="0")
    else:
        return np.format_float_scientific(val, precision=6, trim="0")


class Ecospold1Exporter:
    """Export one or more datasets to Ecospold1 XML.

    The combination of `Ecospold1DataExtractor` and `Ecospold1Exporter` does
    not give prefect roundtrip data flow, especially if data if coming from
    closed-source software with unspecified behaviour. The following
    differences have been observed:

    * This class uses an updated Ecospold1 set of XSDs from https://github.com/sami-m-g/pyecospold/tree/main/pyecospold/schemas/v1
    * The dataset `generator` field is different (`bw2io`)
    * The dataset `number` is not preserved
    * Number formatting is different. We round numbers like `10.2000000000000011` to `10.2`, and always keep at least one decimal point.
    * We always include an `uncertaintyType` for exchanges
    * We always include an `infrastructureProcess` for exchanges (default `false`)
    * The field `dataGeneratorAndPublication` is not used consistently - we always fill this with dummy data.
    * We eliminate duplicate identical `person` elements
    * `person` and `source` elements can be renumbered, but references are kept correct

    """

    def __init__(self, schema_location: Union[str, None] = None):
        self.root = etree.Element(
            "ecoSpold",
            {
                attr_qname: schema_location
                or "https://raw.githubusercontent.com/sami-m-g/pyecospold/main/pyecospold/schemas/v1/EcoSpold01Dataset.xsd"
            },
            nsmap=nsmap,
        )
        self.count = 0

    def add_dataset(self, node: dict) -> None:
        self.count += 1
        tags = dict(node.get("tags", []))
        timestamp = tags.get("ecoSpold01timestamp", datetime.now().isoformat())

        dataset = etree.SubElement(
            self.root,
            "dataset",
            attrib={
                "validCompanyCodes": "CompanyCodes.xml",
                "validRegionalCodes": "RegionalCodes.xml",
                "validCategories": "Categories.xml",
                "validUnits": "Units.xml",
                # Can't guarantee that datasets come from same source
                # so input numbers aren't useful.
                # We reset the exchange numbers as well.
                # They can't be used in any case as they aren't implemented
                # consistently by different LCA software.
                "number": str(self.count),
                "timestamp": timestamp,
                "generator": f"bw2io {version}",
            },
        )
        meta_information = etree.SubElement(
            dataset,
            "metaInformation",
        )

        category = tags.get("ecoSpold01category", "")
        subcategory = tags.get("ecoSpold01subCategory", "")
        comments = node.get("comments", {})

        process_information = etree.SubElement(meta_information, "processInformation")
        etree.SubElement(
            process_information,
            "referenceFunction",
            attrib={
                "datasetRelatesToProduct": bool_to_text(
                    tags.get("ecoSpold01datasetRelatesToProduct", True)
                ),
                "name": node["name"],
                "localName": tags.get("ecoSpold01localName", node["name"]),
                "infrastructureProcess": bool_to_text(
                    tags.get("ecoSpold01infrastructureProcess")
                ),
                # This makes no sense, this number is defined in the relevant exchange
                # "Within the ecoinvent quality network the amount of the reference flow always equals 1."
                "amount": "1",
                "unit": node["unit"],
                "category": category,
                "subCategory": subcategory,
                "localCategory": tags.get("ecoSpold01localCategory", category),
                "localSubCategory": tags.get("ecoSpold01localSubCategory", subcategory),
                "includedProcesses": comments.get("includedProcesses", ""),
                "generalComment": comments.get("generalComment", ""),
                "infrastructureIncluded": bool_to_text(
                    tags.get("ecoSpold01infrastructureIncluded")
                ),
            },
        )
        etree.SubElement(
            process_information,
            "geography",
            attrib={
                "location": node.get("location", "GLO"),
                "text": stripper(comments.get("location", ""), "Location: "),
            },
        )
        etree.SubElement(
            process_information,
            "technology",
            attrib={"text": stripper(comments.get("technology", ""), "Technology: ")},
        )
        time_period = etree.SubElement(
            process_information,
            "timePeriod",
            attrib={
                "text": stripper(comments.get("timePeriod", ""), "Time period: "),
                "dataValidForEntirePeriod": bool_to_text(
                    tags.get("ecoSpold01dataValidForEntirePeriod", True)
                ),
            },
        )
        start = etree.SubElement(time_period, "startDate")
        start.text = tags.get("ecoSpold01startDate", "1970-01-01")
        end = etree.SubElement(time_period, "endDate")
        end.text = tags.get("ecoSpold01endDate", "1970-01-01")
        etree.SubElement(
            process_information,
            "dataSetInformation",
            attrib={
                "type": str(tags.get("ecoSpold01type", "1")),
                "impactAssessmentResult": bool_to_text(
                    tags.get("ecoSpold01impactAssessmentResult")
                ),
                "timestamp": timestamp,
                "version": tags.get("ecoSpold01version", "0.0"),
                "internalVersion": tags.get("ecoSpold01internalVersion", "0.0"),
                "energyValues": str(tags.get("ecoSpold01energyValues", "0")),
                "languageCode": tags.get("ecoSpold01languageCode", "en"),
                "localLanguageCode": tags.get("ecoSpold01localLanguageCode", "de"),
            },
        )
        m_and_v = etree.SubElement(meta_information, "modellingAndValidation")
        etree.SubElement(
            m_and_v,
            "representativeness",
            attrib={
                "productionVolume": stripper(
                    comments.get("productionVolume", "unknown"), "Production volume: "
                ),
                "samplingProcedure": stripper(
                    comments.get("sampling", "unknown"), "Sampling: "
                ),
                "extrapolations": stripper(
                    comments.get("extrapolations", "unknown"), "Extrapolations: "
                ),
                "uncertaintyAdjustments": stripper(
                    comments.get("uncertaintyAdjustments", "unknown"),
                    "Uncertainty adjustments: ",
                ),
            },
        )

        SOURCE_MAP: Dict[str, str] = {
            "Undefined (default)": "0",
            "Article": "1",
            "Chapters in anthology": "2",
            "Seperate publication": "3",
            "Measurement on site": "4",
            "Oral communication": "5",
            "Personal written communication": "6",
            "Questionnaries": "7",
        }

        SOURCE_FIELDS = {
            "nameOfEditors": "editors",
            "pageNumbers": "pages",
            "year": "year",
            "title": "title",
            "titleOfAnthology": "anthology",
            "placeOfPublications": "place_of_publication",
            "publisher": "publisher",
            "journal": "journal",
            "volumeNo": "volume",
            "issueNo": "issue",
            "text": "text",
        }

        for index, source in enumerate(node.get("references", [])):
            etree.SubElement(
                m_and_v,
                "source",
                attrib={
                    "number": str(source.get("identifier", index + 1)),
                    "sourceType": SOURCE_MAP.get(source.get("type"), "0"),
                    "firstAuthor": source.get("authors", [""])[0],
                    "additionalAuthors": (
                        source["authors"][1]
                        if len(source.get("authors", [])) > 1
                        else ""
                    ),
                }
                | {
                    k: str(source.get(v))
                    for k, v in SOURCE_FIELDS.items()
                    if source.get(v)
                },
            )

        admin = etree.SubElement(meta_information, "administrativeInformation")
        etree.SubElement(
            admin,
            "dataEntryBy",
            attrib={
                "number": str(source.get("identifier", index + 1)),
                "qualityNetwork": "1",
            },
        )
        etree.SubElement(
            admin,
            "dataGeneratorAndPublication",
            attrib={
                "person": str(
                    node.get("authors", {}).get("data_entry", {}).get("identifier", 1)
                ),
                "dataPublishedIn": "1",
                "referenceToPublishedSource": "1",
                "accessRestrictedTo": "0",
                "copyright": "true",
            },
        )

        PERSON_FIELDS = [
            ("identifier", "number", "1"),
            ("address", "address", ""),
            ("company", "companyCode", ""),
            ("country", "countryCode", ""),
            ("email", "email", ""),
            ("name", "name", ""),
        ]

        for person in node.get("authors", {}).get("people", []):
            etree.SubElement(
                admin,
                "person",
                attrib={b: str(person.get(a, c)) for a, b, c in PERSON_FIELDS},
            )

        RESOURCES = {
            "natural resource",
            "natural resources",
            "resource",
            "resources",
            "raw",
        }

        UNCERTAINTY_MAPPING = {
            None: "0",
            NoUncertainty.id: "0",
            UndefinedUncertainty.id: "0",
            LognormalUncertainty.id: "1",
            TriangularUncertainty.id: "3",
            UniformUncertainty.id: "4",
        }

        EXCHANGE_FIELDS = {
            "generalComment": "comment",
            "CASNumber": "CAS number",
            "location": "location",
            "formula": "chemical formula",
            "referenceToSource": "source_reference",
            "pageNumbers": "pages",
        }

        flow_data = etree.SubElement(dataset, "flowData")
        for index, exc in enumerate(node.get("exchanges", [])):
            attrs = {
                "number": str(index + 1),
                "unit": str(exc.get("unit")),
                "name": exc.get("name", ""),
                "meanValue": pretty_number(exc["amount"]),
                "infrastructureProcess": bool_to_text(exc.get("infrastructureProcess")),
            } | {k: exc.get(v) for k, v in EXCHANGE_FIELDS.items() if exc.get(v)}

            if exc.get("uncertainty type") is not None:
                attrs["uncertaintyType"] = UNCERTAINTY_MAPPING.get(
                    exc.get("uncertainty type")
                )
            if exc.get("categories") and exc["categories"][0]:
                attrs["category"] = exc["categories"][0] or ""
            if len(exc.get("categories")) > 1 and exc["categories"][1]:
                attrs["subCategory"] = exc["categories"][1] or ""

            if exc.get("uncertainty type") == LognormalUncertainty.id and exc.get(
                "scale"
            ):
                attrs["standardDeviation95"] = pretty_number(np.exp(exc["scale"]) ** 2)
            elif exc.get("uncertainty type") == NormalUncertainty.id and exc.get(
                "scale"
            ):
                attrs["standardDeviation95"] = pretty_number(exc["scale"] * 2)

            if exc.get("minimum"):
                attrs["minValue"] = pretty_number(exc["minimum"])
            if exc.get("maximum"):
                attrs["maxValue"] = pretty_number(exc["maximum"])

            exc_element = etree.SubElement(
                flow_data,
                "exchange",
                attrib=attrs,
            )
            if exc["type"] == "technosphere":
                elem = etree.SubElement(exc_element, "inputGroup")
                elem.text = "5"
            elif exc["type"] == "production":
                elem = etree.SubElement(exc_element, "outputGroup")
                elem.text = "0"
            elif exc["type"] == "substitution":
                elem = etree.SubElement(exc_element, "outputGroup")
                elem.text = "1"
            elif exc["type"] == "biosphere":
                if exc["categories"][0].lower() in RESOURCES:
                    elem = etree.SubElement(exc_element, "inputGroup")
                    elem.text = "5"
                else:
                    elem = etree.SubElement(exc_element, "outputGroup")
                    elem.text = "4"
            else:
                raise ValueError("Can't map exchange type {}".format(exc["type"]))

    @property
    def bytes(self) -> bytes:
        return etree.tostring(
            self.root, encoding="utf-8", xml_declaration=True, pretty_print=True
        )

    def __repr__(self) -> str:
        return self.bytes.decode("utf-8")

    def write_to_file(self, filepath: Path) -> None:
        with open(filepath, "wb") as f:
            f.write(self.bytes)
