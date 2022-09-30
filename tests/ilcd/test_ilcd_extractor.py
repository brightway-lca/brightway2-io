from bw2io.extractors.ilcd import extract_zip, get_xml_value, xpaths
from pathlib import Path
from lxml.etree import _Element

example_file = Path(".").absolute().parent.parent / "bw2io/data/examples/ilcd-example.zip"

def test_extract_zip():

    trees = extract_zip(example_file)

    # assure completeness
    assert len(trees) == 1240

    # assure that all return values are etrees
    assert all([isinstance(t, _Element) for t in trees.values()])

    return trees


def test_xml_value_getter():
    trees = extract_zip(example_file)
    tree_object = list(trees.values())[0]
    general_namespace = {"n": "http://lca.jrc.it/ILCD/Flow"}
    namespaces = {"common": "http://lca.jrc.it/ILCD/Common"}
    namespaces.update(general_namespace)
    xpath_str = xpaths["value"]
    v = get_xml_value(tree_object, xpath_str, general_namespace, namespaces)
    assert v == '1.0'

if __name__ == "__main__":
    test_extract_zip()
    test_xml_value_getter()