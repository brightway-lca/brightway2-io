from bw2io.extractors.json_ld import JSONLDExtractor
from bw2io.strategies import (
    json_ld_get_normalized_exchange_locations,
    json_ld_get_normalized_exchange_units,
    json_ld_get_activities_list_from_rawdata,
    # json_ld_add_activity_unit,
    json_ld_convert_unit_to_reference_unit,
    json_ld_rename_metadata_fields,
    json_ld_label_exchange_type,
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


def test_exchange_units():
    data = JSONLDExtractor.extract(FIXTURES)
    data = json_ld_convert_unit_to_reference_unit(data)
    data = json_ld_get_activities_list_from_rawdata(data)
    assert {
       exc["unit"]
       for act in data
       for exc in act["exchanges"]
    } == {'kg', 't*km', 'm2*a', 'Item(s)', 'm3', 'MJ'}
    data = json_ld_get_normalized_exchange_units(data)
    assert {
       exc["unit"]
       for act in data
       for exc in act["exchanges"]
    } == {'megajoule', 'ton kilometer', 'cubic meter', 'kilogram', 'square meter-year', 'unit'}


def test_activities_list():
    data = JSONLDExtractor.extract(FIXTURES)
    db = json_ld_get_activities_list_from_rawdata(data)
    assert len(data['processes']) == len(db)
    for i,key in enumerate(data['processes'].keys()):
        assert key == db[i]['@id']


def test_conversion_to_ref_unit():
    data = JSONLDExtractor.extract(FIXTURES)

    assert data['processes']['1b97b691-7c00-4150-9e97-df2020bfd203']['exchanges'][3]['amount'] == 1623.0

    data = json_ld_convert_unit_to_reference_unit(data)

    for act in data['processes'].values():
        for exc in act['exchanges']:
            assert isinstance(exc['unit'], str)
            assert 'refUnit' not in exc['flow']

    assert data['processes']['1b97b691-7c00-4150-9e97-df2020bfd203']['exchanges'][3]['amount'] == 1623.0 * 1000

# def test_activity_unit():
#     data = JSONLDExtractor.extract(FIXTURES)
#     data = json_ld_get_normalized_exchange_locations(data)
#     data = json_ld_get_normalized_exchange_units(data)
#     db = json_ld_get_activities_list_from_rawdata(data)
#     db = json_ld_add_activity_unit(db)
#     print('Here')
#     print([ds['unit'] for ds in db])
# TODO what if no production excs or multiple?

def test_metadata_fields():
    data = JSONLDExtractor.extract(FIXTURES)
    db = json_ld_get_activities_list_from_rawdata(data)
    db = json_ld_rename_metadata_fields(db)
    assert db[5]['code'] == '2fc8aa4b-481d-4302-bd9d-b5b7afcb3ad6'
    assert not db[4].get('@id', False)
    assert db[3].get('classifications', False)
    assert not db[2].get('category', False)


def test_basic_exchange_type_labelling():
    data = list(JSONLDExtractor.extract(FIXTURES)['processes'].values())
    # data = json_ld_convert_db_dict_into_list(data)
    data = json_ld_label_exchange_type(data)

    assert all(exc.get('type') for act in data for exc in act['exchanges'])
