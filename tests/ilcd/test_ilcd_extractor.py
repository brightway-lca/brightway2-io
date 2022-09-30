from bw2io.extractors.ilcd import extract_zip
from pathlib import Path
from lxml.etree import _Element

def test_extract_zip():
    file = Path(".").absolute().parent.parent / "bw2io/data/examples/ilcd-example.zip"
    trees = extract_zip(file)

    # assure completeness
    assert len(trees) == 1240

    # assure that all return values are etrees
    assert all([isinstance(t, _Element) for t in trees.values()])
    pass


def test_parser():
    fp_process = "/Users/michael.baer/Dropbox (On)/Michaels Data/ILCD-hackaton/ILCD/processes/d2fe899e-7fc0-49d3-a7cc-bbf8cad5439a_00.00.001.xml"
    fp_flows = "/Users/michael.baer/Dropbox (On)/Michaels Data/ILCD-hackaton/ILCD/flows/0a51e24a-6201-47bb-b8f2-eb52bca60c83_03.00.004.xml"
    tree_object = etree.parse(open(fp_flows, encoding="utf-8"))

    general_namespace = {"n": "http://lca.jrc.it/ILCD/Flow"}
    namespaces = {"common": "http://lca.jrc.it/ILCD/Common"}
    namespaces.update(general_namespace)
    xpath_str = xpaths["value"]
    get_xml_value(tree_object, xpath_str, general_namespace, namespaces)

if __name__ == "__main__":
    test_extract_zip()