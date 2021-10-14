from bw2data import Database
from bw2data.tests import bw2test

from bw2io import create_default_biosphere3


@bw2test
def test_ensure_base_biosphere_flows_have_tuples():
    create_default_biosphere3()
    for flow in Database("biosphere3"):
        if type(flow["categories"]) != tuple:
            print(flow)
    assert all(type(flow["categories"]) == tuple for flow in Database("biosphere3"))
