from bw2io.extractors.ilcd import (
    extract_zip,
    get_xml_value,
    extract_all_relevant_info,
    xpaths_process,
    xpaths_flows,
    namespaces,
    apply_xpaths_to_xml_file,
)
from pathlib import Path
from lxml.etree import _Element

example_file = (
    Path(__file__).absolute().parent.parent.parent
    / "bw2io/data/examples/ilcd-example.zip"
)


def test_extract_zip():

    trees = extract_zip(example_file)
    # assure completeness
    assert len(trees) == 2

    # assure that all return values are etrees
    for branches in trees:
        assert all(
            [isinstance(trees[branches][t], _Element) for t in trees[branches]]
        )

    return trees


def test_xml_value_getter():
    trees = extract_zip(example_file)
    tree_object = trees["processes"][list(trees["processes"])[0]]
    xpath_str = xpaths_process["basename"]
    v = get_xml_value(
        tree_object,
        xpath_str,
        namespaces["default_process_ns"],
        namespaces["others"],
    )
    assert v == "Aromatic Polyester Polyols (APP) production mix"


def test_apply_xpaths_to_xml_file():
    trees = extract_zip(example_file)
    tree_object = trees["processes"][list(trees["processes"])[0]]
    v = apply_xpaths_to_xml_file(xpaths_process, tree_object)
    v_expected = {
        "basename": "Aromatic Polyester Polyols (APP) production mix",
        "treatment_standards_routes": "polycondensation",
        "mix_and_location_types": "production mix, at producer",
        "functional_unit_flow_properties": "Hydroxyl value: 150-360 mg KOH/g, aromatic content: 5-50%; Average gross calorific value 22.5 MJ/kg",
        "uuid": "d2fe899e-7fc0-49d3-a7cc-bbf8cad5439a",
        "reference_year": "2019",
        "data_set_valid_until": "2026",
        "location": "EU-28",
        "reference_to_reference_flow": None,
    }
    # do not compare exchanges because too much data
    assert all(v_expected[k] == v[k] for k in v_expected)


def test_extract_all_relevant_info():
    v = extract_all_relevant_info(example_file)
    print(v["flows"])
    pass


if __name__ == "__main__":
    # test_extract_zip()
    # test_xml_value_getter()
    test_apply_xpaths_to_xml_file()
    test_extract_all_relevant_info()
