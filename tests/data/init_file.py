from bw2io.data import get_valid_geonames


def test_geodata():
    assert "WECC, US only" in get_valid_geonames()
    assert "Québec, HQ distribution network" in get_valid_geonames()
