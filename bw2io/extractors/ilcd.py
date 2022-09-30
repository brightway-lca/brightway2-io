import zipfile
from pathlib import Path

from lxml import etree

# ILCD should be read in a particular order
sort_order = {
"contacts": 0,
"sources":1,
"unitgroups":2,
"flowproperties": 3,
"flows":4,
"processes":5,
"external_docs": 6,
}

# for the moment we ignore some of the folders
to_ignore = ['contacts', 'sources',
'unitgroups', 'flowproperties',
'external_docs',"processes"]

examples_path = Path(__file__).parent.parent / 'data' /'examples'/"ilcd_example.zip"

with zipfile.ZipFile(examples_path, mode="r") as archive:
    filelist = archive.filelist

    # remove folders that we do not need
    filelist = [file for file in filelist if Path(file.filename).parts[1] 
    not in to_ignore]

    # sort by folder
    filelist = sorted(filelist,key=lambda x:sort_order.get(Path(x.filename).parts[1]))


    for file in filelist:
        f = archive.read(file)
        tree = etree.fromstring(f)
        break


def get_xml_value(xml_tree, xpath_str, general_ns, namespaces):
    assert len(general_ns)==1, "The general namespace is not clearly defined."
    # Adding the general namespace name to xpath expression
    xpath_segments = xpath_str.split('/')
    for i in range(len(xpath_segments)):
        if ':' not in xpath_segments[i] and '(' not in xpath_segments[i] and '@' not in xpath_segments[i][:1] and '' != xpath_segments[i]:
            xpath_segments[i] = "n:" + xpath_segments[i]
    xpath_str = "/".join(xpath_segments)
    r = xml_tree.xpath(xpath_str,namespaces=namespaces)
    assert len(r)==1, "Unexpected results from XML parsing: " + xpath_str
    return r[0]


# Example xpath_str for flows
xpath_str = '/flowDataSet/flowInformation/dataSetInformation/name/baseName'
xpath_str = '/flowDataSet/flowInformation/dataSetInformation/common:UUID'
xpath_str = '/flowDataSet/flowInformation/dataSetInformation/classificationInformation/common:elementaryFlowCategorization/common:category[@level=2]'
xpath_str = '/flowDataSet/modellingAndValidation/LCIMethod/typeOfDataSet'
xpath_str = '/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/meanValue/text()'
#xpath_str = '/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/referenceToFlowPropertyDataSet/@refObjectId'

# Example code for the function (get_xml_value)

fp_process = '/Users/michael.baer/Dropbox (On)/Michaels Data/ILCD-hackaton/ILCD/processes/d2fe899e-7fc0-49d3-a7cc-bbf8cad5439a_00.00.001.xml'
fp_flows = '/Users/michael.baer/Dropbox (On)/Michaels Data/ILCD-hackaton/ILCD/flows/0a51e24a-6201-47bb-b8f2-eb52bca60c83_03.00.004.xml'
tree_object = etree.parse(open(fp_flows, encoding="utf-8"))

general_namespace = {'n':'http://lca.jrc.it/ILCD/Flow'}
namespaces = {'common':'http://lca.jrc.it/ILCD/Common'}
namespaces.update(general_namespace)
xpath_str = '/flowDataSet/flowProperties/flowProperty[@dataSetInternalID=/flowDataSet/flowInformation/quantitativeReference/referenceToReferenceFlowProperty/text()]/meanValue/text()'

get_xml_value(tree_object, xpath_str, general_namespace, namespaces)
