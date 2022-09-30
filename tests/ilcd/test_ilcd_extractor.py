from bw2io.extractors.ilcd import extract_zip, get_xml_value, xpaths_process, xpaths_process_exchanges, xpaths_flows, namespaces, apply_xpaths_to_process_xml_file
from pathlib import Path
from lxml.etree import _Element

example_file = Path(__file__).absolute().parent.parent.parent / "bw2io/data/examples/ilcd-example.zip"

def test_extract_zip():

    trees = extract_zip(example_file)
    # assure completeness
    assert len(trees) == 2

    # assure that all return values are etrees
    for branches in trees:
        assert all([isinstance(trees[branches][t], _Element) for t in trees[branches]])

    return trees


def test_xml_value_getter():
    trees = extract_zip(example_file)
    tree_object = trees['processes'][list(trees['processes'])[0]]
    default_ns = namespaces["default_process_ns"]
    ns = namespaces["others"]
    ns.update(default_ns)
    xpath_str = xpaths_process["basename"]
    v = get_xml_value(tree_object, xpath_str, default_ns, ns)
    assert v.text == "Aromatic Polyester Polyols (APP) production mix"

def test_apply_xpaths_to_process_xml_file():
    trees = extract_zip(example_file)
    tree_object = trees['processes'][list(trees['processes'])[0]]
    v = apply_xpaths_to_process_xml_file(xpaths_process, tree_object)
    pass


if __name__ == "__main__":
    # test_extract_zip()
    # test_xml_value_getter()
    test_apply_xpaths_to_process_xml_file()