import zipfile
from pathlib import Path
from typing import Union

from lxml import etree

# Xpath for values in process XML file will return one value in a list
xpaths_process = {
    "basename": "/processDataSet/processInformation/dataSetInformation/name/baseName/text()",
    "treatment_standards_routes": "/processDataSet/processInformation/dataSetInformation/name/treatmentStandardsRoutes/text()",
    "mix_and_location_types": "/processDataSet/processInformation/dataSetInformation/name/mixAndLocationTypes/text()",
    "functional_unit_flow_properties": "/processDataSet/processInformation/dataSetInformation/name/functionalUnitFlowProperties/text()",
    "uuid": "/processDataSet/processInformation/dataSetInformation/common:UUID/text()",
    "reference_year": "/processDataSet/processInformation/time/common:referenceYear/text()",
    "data_set_valid_until": "/processDataSet/processInformation/time/common:dataSetValidUntil/text()",
    "location": "/processDataSet/processInformation/geography/locationOfOperationSupplyOrProduction/@location",
    "reference_to_reference_flow": "/processDataSet/exchanges/exchange[@dataSetInternalID=/processDataSet/processInformation/quantitativeReference/referenceToReferenceFlow]",
    
    # Xpath for values in process XML file, will return multiple values as a list
    "exchanges_internal_id": "/processDataSet/exchanges/exchange/@dataSetInternalID",
    "exchanges_name": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/common:shortDescription/text()",
    "exchanges_uuid": "/processDataSet/exchanges/exchange/referenceToFlowDataSet/@refObjectId",
    "exchanges_direction": "/processDataSet/exchanges/exchange/exchangeDirection/text()",
    "exchanges_amount": "/processDataSet/exchanges/exchange/resultingAmount/text()",
}

# Xpath for values in flow XML files, will return one values in a list
xpaths_flows = {
    "basename": "/flowDataSet/flowInformation/dataSetInformation/name/baseName/text()",
    "uuid": "/flowDataSet/flowInformation/dataSetInformation/common:UUID/text()",
    "category": "/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=2]/text()",
    "type": "/flowDataSet/modellingAndValidation/LCIMethod/typeOfDataSet/text()",
    "value": "/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/meanValue/text()",
    "refobj": "/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/referenceToFlowPropertyDataSet/@refObjectId",
}

# Namespaces to use with the XPath
namespaces = {
    "default_process_ns": {"pns": "http://lca.jrc.it/ILCD/Process"},
    "default_flow_ns": {"fns": "http://lca.jrc.it/ILCD/Flow"},
    "others": {"common": "http://lca.jrc.it/ILCD/Common"},
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
        # "processes",
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
            file_type = Path(file.filename).parts[1]
            if file_type not in trees:
                trees[file_type] = {}
            f = archive.read(file)
            trees[file_type][file.filename] = etree.fromstring(f)

    return trees

def extract_all_relevant_info(file_path):
    trees = extract_zip(file_path)
    file_types = ['processes','flows']
    results = {}
    for ft in file_types:
        results[ft] = []
        for exc_f in trees[ft]:
            tree_object = trees[ft][exc_f]
            if ft == 'processes':
                results[ft].append(apply_xpaths_to_xml_file(xpaths_process, tree_object))
            if ft == 'flows':
                results[ft].append(apply_xpaths_to_xml_file(xpaths_flows, tree_object))
    return results

def apply_xpaths_to_xml_file(xpath_dict, xml_tree):
    results = {}
    for k in xpath_dict:
        default_ns = namespaces["default_process_ns"] if "flowDataSet" not in list(xpath_dict.items())[0][1] else namespaces["default_flow_ns"]
        results[k] = get_xml_value(
            xml_tree, xpath_dict[k], default_ns, namespaces["others"])
    return results


def get_xml_value(xml_tree, xpath_str, default_ns, namespaces):
    assert (
        len(default_ns) == 1
    ), "The general namespace is not clearly defined."
    namespaces.update(default_ns)

    # Adding the general namespace name to xpath expression
    xpath_segments = xpath_str.split("/")
    namespace_abbrevation = list(default_ns.keys())[0]
    for i in range(len(xpath_segments)):
        if (
            ":" not in xpath_segments[i]
            and "(" not in xpath_segments[i]
            and "@" not in xpath_segments[i][:1]
            and "" != xpath_segments[i]
        ):
            xpath_segments[i] = namespace_abbrevation + ":" + xpath_segments[i]
    xpath_str = "/".join(xpath_segments)
    r = xml_tree.xpath(xpath_str, namespaces=namespaces)
    #assert len(r) <= 1, "Unexpected results from XML parsing: " + xpath_str + ", " + str(len(r))
    if len(r)==0: return None
    if len(r)==1: return r[0]
    return r
