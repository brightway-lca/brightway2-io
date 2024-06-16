from pathlib import Path

from bw2io.data import add_suffix, get_valid_geonames


def test_geodata():
    assert "WECC, US only" in get_valid_geonames()
    assert "Québec, HQ distribution network" in get_valid_geonames()


def test_suffixes():
    assert add_suffix("foo", "bar") == Path("foobar")
    assert add_suffix(Path("foo"), ".bar") == Path("foo.bar")
    assert add_suffix(Path("foo.bar"), ".baz") == Path("foo.bar.baz")
