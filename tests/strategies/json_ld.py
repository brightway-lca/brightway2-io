from bw2io.extractors.json_ld import JSONLDExtractor
from bw2io.strategies import (
    json_ld_get_normalized_exchange_locations,
    json_ld_label_exchange_type,
    json_ld_convert_db_dict_into_list,
)
from pathlib import Path


FIXTURES = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "json-ld"
    / "beef-cattle-finishing"
)


def test_extraction():
    data = JSONLDExtractor.extract(FIXTURES)
    print(sorted(data.keys()))
    assert sorted(data.keys()) == sorted(
        [
            "processes",
            "dq_systems",
            "locations",
            "actors",
            "flow_properties",
            "product_systems",
            "sources",
            "unit_groups",
            "categories",
            "flows",
        ]
    )


def test_exchange_locations():
    data = JSONLDExtractor.extract(FIXTURES)
    assert {
        exc["flow"].get("location")
        for act in data["processes"].values()
        for exc in act["exchanges"]
    } == {"US", None, "NL", "DZ", "RNA"}
    data = json_ld_get_normalized_exchange_locations(data)
    assert {
        exc["flow"].get("location")
        for act in data["processes"].values()
        for exc in act["exchanges"]
    } == {"Northern America", "United States", "Algeria", None, "Netherlands"}


def test_basic_exchange_type_labelling():
    data = list(JSONLDExtractor.extract(FIXTURES)['processes'].values())
    # data = json_ld_convert_db_dict_into_list(data)
    data = json_ld_label_exchange_type(data)

    assert all(exc.get('type') for act in data for exc in act['exchanges'])
