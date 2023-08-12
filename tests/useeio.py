import bw2data as bd
import pytest
from bw2data.tests import bw2test

from bw2io import useeio11


@pytest.mark.slow
@bw2test
def test_useeio_import():
    useeio11("foo")
    assert "foo" in bd.databases
    assert len(bd.Database("foo")) > 2000


@pytest.mark.slow
@bw2test
def test_useeio_import_collapse_products():
    useeio11("foo", collapse_products=True)
    assert "foo" in bd.databases
    assert len(bd.Database("foo")) > 2000
    assert not sum(1 for ds in bd.Database("foo") if ds["type"] == "product")
    assert 300 < sum(1 for ds in bd.Database("foo") if ds["type"] == "process") < 400
    assert (
        sum(
            len(ds.technosphere())
            for ds in bd.Database("foo")
            if ds["type"] == "process"
        )
        > 70_000
    )


@pytest.mark.slow
@bw2test
def test_useeio_import_prune():
    useeio11("foo", collapse_products=True, prune=True)
    assert "foo" in bd.databases
    assert len(bd.Database("foo")) > 2000
    assert not sum(1 for ds in bd.Database("foo") if ds["type"] == "product")
    assert 300 < sum(1 for ds in bd.Database("foo") if ds["type"] == "process") < 400
    assert (
        sum(
            len(ds.technosphere())
            for ds in bd.Database("foo")
            if ds["type"] == "process"
        )
        < 50_000
    )
