from pathlib import Path

import pytest

from bw2io.errors import UnallocatableDataset
from bw2io.extractors.json_ld import JSONLDExtractor
from bw2io.strategies import (  # json_ld_add_activity_unit,
    json_ld_allocate_datasets,
    json_ld_convert_unit_to_reference_unit,
    json_ld_get_activities_list_from_rawdata,
    json_ld_get_normalized_exchange_locations,
    json_ld_get_normalized_exchange_units,
    json_ld_label_exchange_type,
    json_ld_rename_metadata_fields,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "json-ld"

CATTLE = FIXTURES / "beef-cattle-finishing"
FPL = FIXTURES / "US-FPL"


def test_extraction():
    data = JSONLDExtractor.extract(CATTLE)
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
    data = JSONLDExtractor.extract(CATTLE)
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


def test_exchange_units():
    data = JSONLDExtractor.extract(CATTLE)
    data = json_ld_convert_unit_to_reference_unit(data)
    data = json_ld_get_activities_list_from_rawdata(data)
    assert {exc["unit"] for act in data for exc in act["exchanges"]} == {
        "kg",
        "t*km",
        "m2*a",
        "Item(s)",
        "m3",
        "MJ",
    }
    data = json_ld_get_normalized_exchange_units(data)
    assert {exc["unit"] for act in data for exc in act["exchanges"]} == {
        "megajoule",
        "ton kilometer",
        "cubic meter",
        "kilogram",
        "square meter-year",
        "unit",
    }


def test_activities_list():
    data = JSONLDExtractor.extract(CATTLE)
    db = json_ld_get_activities_list_from_rawdata(data)
    assert len(data["processes"]) == len(db)
    for i, key in enumerate(data["processes"].keys()):
        assert key == db[i]["@id"]


def test_conversion_to_ref_unit():
    data = JSONLDExtractor.extract(CATTLE)

    assert (
        data["processes"]["1b97b691-7c00-4150-9e97-df2020bfd203"]["exchanges"][3][
            "amount"
        ]
        == 1623.0
    )

    data = json_ld_convert_unit_to_reference_unit(data)

    for act in data["processes"].values():
        for exc in act["exchanges"]:
            assert isinstance(exc["unit"], str)
            assert "refUnit" not in exc["flow"]

    assert (
        data["processes"]["1b97b691-7c00-4150-9e97-df2020bfd203"]["exchanges"][3][
            "amount"
        ]
        == 1623.0 * 1000
    )


# def test_activity_unit():
#     data = JSONLDExtractor.extract(CATTLE)
#     data = json_ld_get_normalized_exchange_locations(data)
#     data = json_ld_get_normalized_exchange_units(data)
#     db = json_ld_get_activities_list_from_rawdata(data)
#     db = json_ld_add_activity_unit(db)
#     print('Here')
#     print([ds['unit'] for ds in db])
# TODO what if no production excs or multiple?


def test_allocation_default():
    data = JSONLDExtractor.extract(FPL)
    assert "64dec5f5-ce97-40f2-a767-2fc665dfb473" in data["processes"]
    assert (
        "64dec5f5-ce97-40f2-a767-2fc665dfb473.7bd7a980-2f60-4830-8a6b-7e75cc0d7892"
        not in data["processes"]
    )
    assert data["processes"]["64dec5f5-ce97-40f2-a767-2fc665dfb473"][
        "allocationFactors"
    ]
    assert (
        len(data["processes"]["64dec5f5-ce97-40f2-a767-2fc665dfb473"]["exchanges"]) == 7
    )

    data = json_ld_allocate_datasets(data)

    assert "64dec5f5-ce97-40f2-a767-2fc665dfb473" not in data["processes"]

    assert not data["processes"][
        "64dec5f5-ce97-40f2-a767-2fc665dfb473.7bd7a980-2f60-4830-8a6b-7e75cc0d7892"
    ]["allocationFactors"]
    assert len(
        data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.7bd7a980-2f60-4830-8a6b-7e75cc0d7892"
        ]["exchanges"]
    ) == (7 - 2)
    assert all(
        exc["amount"] in (0, 159.0)
        for exc in data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.7bd7a980-2f60-4830-8a6b-7e75cc0d7892"
        ]["exchanges"]
    )

    assert not data["processes"][
        "64dec5f5-ce97-40f2-a767-2fc665dfb473.8afb5cc3-26fc-416c-991e-afe07bb06bd4"
    ]["allocationFactors"]
    assert len(
        data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.8afb5cc3-26fc-416c-991e-afe07bb06bd4"
        ]["exchanges"]
    ) == (7 - 2)
    assert all(
        exc["amount"] in (0, 11.2)
        for exc in data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.8afb5cc3-26fc-416c-991e-afe07bb06bd4"
        ]["exchanges"]
    )

    assert not data["processes"][
        "64dec5f5-ce97-40f2-a767-2fc665dfb473.7d1ffffe-eac6-4e83-9b69-3ded4a3193a9"
    ]["allocationFactors"]
    assert len(
        data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.7d1ffffe-eac6-4e83-9b69-3ded4a3193a9"
        ]["exchanges"]
    ) == (7 - 2)
    assert all(
        exc["amount"] in (0, 36.4)
        for exc in data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.7d1ffffe-eac6-4e83-9b69-3ded4a3193a9"
        ]["exchanges"]
    )


def get_exchange_dict(ds, exclude=None):
    return {
        exc["flow"]["@id"]: exc["amount"]
        for exc in ds["exchanges"]
        if exc["flow"]["@id"] != exclude
    }


def test_allocation_physical():
    data = JSONLDExtractor.extract(FPL)
    original = get_exchange_dict(
        data["processes"]["64dec5f5-ce97-40f2-a767-2fc665dfb473"]
    )

    data = json_ld_allocate_datasets(data, "PHYSICAL_ALLOCATION")
    assert "64dec5f5-ce97-40f2-a767-2fc665dfb473" not in data["processes"]

    # Syngas
    # 0.825
    result = get_exchange_dict(
        data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.7bd7a980-2f60-4830-8a6b-7e75cc0d7892"
        ],
        "7bd7a980-2f60-4830-8a6b-7e75cc0d7892",
    )
    for k, v in result.items():
        assert v == original[k] * 0.825

    # Tar
    # 0
    print(
        [
            exc["amount"]
            for exc in data["processes"][
                "64dec5f5-ce97-40f2-a767-2fc665dfb473.8afb5cc3-26fc-416c-991e-afe07bb06bd4"
            ]["exchanges"]
        ]
    )
    assert all(
        exc["amount"] in (0, 11.2)
        for exc in data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.8afb5cc3-26fc-416c-991e-afe07bb06bd4"
        ]["exchanges"]
    )

    # Biochar
    # 0.175
    result = get_exchange_dict(
        data["processes"][
            "64dec5f5-ce97-40f2-a767-2fc665dfb473.7d1ffffe-eac6-4e83-9b69-3ded4a3193a9"
        ],
        "7d1ffffe-eac6-4e83-9b69-3ded4a3193a9",
    )
    for k, v in result.items():
        assert v == original[k] * 0.175


def test_allocation_causal():
    data = JSONLDExtractor.extract(FPL)
    original = get_exchange_dict(
        data["processes"]["81e3e71d-0616-32a8-89ce-216aacc03ff5"]
    )

    data = json_ld_allocate_datasets(data, "CAUSAL_ALLOCATION")
    assert "81e3e71d-0616-32a8-89ce-216aacc03ff5" not in data["processes"]

    result = get_exchange_dict(
        data["processes"][
            "81e3e71d-0616-32a8-89ce-216aacc03ff5.12d75c42-10a6-35ab-abf9-f83fcf5f7b21"
        ],
        "12d75c42-10a6-35ab-abf9-f83fcf5f7b21",
    )

    scaling = {
        "8b77e92f-1eaa-3072-afbe-4648b2ddf583": 2,
        "2afb4e2f-b224-3900-956f-03e3f27b276b": 3,
        "06581fb2-1de0-3e78-8298-f37605dea142": 4,
        "0e44e579-abb0-3c77-af64-c774d65be529": 5,
        "7597d8b1-16c8-39c4-bab9-01c327626f08": 6,
        "bf2b1e5a-4c92-3974-a2fd-a68898833086": 0,
    }

    for k, v in result.items():
        assert v == original[k] * scaling[k]


def test_metadata_fields():
    data = JSONLDExtractor.extract(CATTLE)
    db = json_ld_get_activities_list_from_rawdata(data)
    db = json_ld_rename_metadata_fields(db)
    assert db[5]["code"] == "bb4f02fd-2277-400d-92ef-0b712aef4baf"
    assert not db[4].get("@id", False)
    assert db[3].get("classifications", False)
    assert not db[2].get("category", False)


def test_basic_exchange_type_labelling():
    data = list(JSONLDExtractor.extract(CATTLE)["processes"].values())
    # data = json_ld_convert_db_dict_into_list(data)
    data = json_ld_label_exchange_type(data)

    assert all(exc.get("type") for act in data for exc in act["exchanges"])
