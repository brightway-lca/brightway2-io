import zipfile
from pathlib import Path
from typing import Union

from lxml import etree

xpaths = {
    "basename": "/flowDataSet/flowInformation/dataSetInformation/name/baseName",
    "uuid": "/flowDataSet/flowInformation/dataSetInformation/common:UUID",
    "category": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=2]",
    "type": "/flowDataSet/modellingAndValidation/LCIMethod/typeOfDataSet",
    "value": "/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/meanValue/text()",
    "refobj": "/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/referenceToFlowPropertyDataSet/@refObjectId",
}


def extract_zip(path: Union[Path, str] = None):
    # ILCD should be read in a particular order
    sort_order = {
        "contacts": 0,
        "sources": 1,
        "unitgroups": 2,
        "flowproperties": 3,
        "flows": 4,
        "processes": 5,
        "external_docs": 6,
    }

    # for the moment we ignore some of the folders
    to_ignore = [
        "contacts",
        "sources",
        "unitgroups",
        "flowproperties",
        "external_docs",
        "processes",
    ]

    if path is None:
        path = (
            Path(__file__).parent.parent
            / "data"
            / "examples"
            / "ilcd_example.zip"
        )

    with zipfile.ZipFile(path, mode="r") as archive:
        filelist = archive.filelist

        # remove folders that we do not need
        filelist = [
            file
            for file in filelist
            if Path(file.filename).parts[1] not in to_ignore
        ]

        # sort by folder
        filelist = sorted(
            filelist, key=lambda x: sort_order.get(Path(x.filename).parts[1])
        )

        trees = {}
        for file in filelist:
            f = archive.read(file)
            trees[file] = etree.fromstring(f)

    return trees


def get_xml_value(xml_tree, xpath_str, general_ns, namespaces):
    assert (
        len(general_ns) == 1
    ), "The general namespace is not clearly defined."
    # Adding the general namespace name to xpath expression
    xpath_segments = xpath_str.split("/")
    for i in range(len(xpath_segments)):
        if (
            ":" not in xpath_segments[i]
            and "(" not in xpath_segments[i]
            and "@" not in xpath_segments[i][:1]
            and "" != xpath_segments[i]
        ):
            xpath_segments[i] = "n:" + xpath_segments[i]
    xpath_str = "/".join(xpath_segments)
    r = xml_tree.xpath(xpath_str, namespaces=namespaces)
    assert len(r) == 1, "Unexpected results from XML parsing: " + xpath_str
    return r[0]