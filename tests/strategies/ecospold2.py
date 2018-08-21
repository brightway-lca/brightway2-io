from bw2io.strategies.ecospold2 import (
    fix_unreasonably_high_lognormal_uncertainties,
    remove_uncertainty_from_negative_loss_exchanges,
    set_lognormal_loc_value,
    drop_temporary_outdated_biosphere_flows,
)
from stats_arrays import *


def test_fix_unreasonably_high_lognormal_uncertainties():
    db = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'scale': 5,
        }, {
            'uncertainty type': -1,
            'scale': 5,
        }]
    }]
    expected = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'scale': 0.25,
        }, {
            'uncertainty type': -1,
            'scale': 5,
        }]
    }]
    assert fix_unreasonably_high_lognormal_uncertainties(db) == expected

    db = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'scale': 1,
        }]
    }]
    expected = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'scale': 15,
        }]
    }]
    assert fix_unreasonably_high_lognormal_uncertainties(db, 0.5, 15) == expected

def test_set_lognormal_loc_value():
    db = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'loc': 1000,
            'amount': 1,
        }, {
            'uncertainty type': -1,
            'loc': 1000,
            'amount': 1,
        }]
    }]
    expected = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'loc': 0,
            'amount': 1,
        }, {
            'uncertainty type': -1,
            'loc': 1000,
            'amount': 1,
        }]
    }]
    assert set_lognormal_loc_value(db) == expected

def test_remove_uncertainty_from_negative_loss_exchanges():
    db = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'type': 'technosphere',
            'amount': -1,
            'name': 'bar'
        }, {
            'uncertainty type': LognormalUncertainty.id,
            'type': 'technosphere',
            'amount': 1,
            'name': 'foo'
        }, {
            'uncertainty type': LognormalUncertainty.id,
            'type': 'technosphere',
            'amount': -1,
            'name': 'foo',
            'scale': 'something',
        }, {
            'uncertainty type': LognormalUncertainty.id,
            'type': 'production',
            'amount': 1,
            'name': 'foo'
        }, {
            'uncertainty type': -1,
            'type': 'technosphere',
            'amount': 0,
            'name': 'foo'
        }]
    }]
    expected = [{
        'exchanges': [{
            'uncertainty type': LognormalUncertainty.id,
            'type': 'technosphere',
            'amount': -1,
            'name': 'bar'
        }, {
            'uncertainty type': LognormalUncertainty.id,
            'type': 'technosphere',
            'amount': 1,
            'name': 'foo'
        }, {
            'uncertainty type': UndefinedUncertainty.id,
            'type': 'technosphere',
            'amount': -1,
            'loc': -1,
            'name': 'foo'
        }, {
            'uncertainty type': LognormalUncertainty.id,
            'type': 'production',
            'amount': 1,
            'name': 'foo'
        }, {
            'uncertainty type': -1,
            'type': 'technosphere',
            'amount': 0,
            'name': 'foo'
        }]
    }]
    assert remove_uncertainty_from_negative_loss_exchanges(db) == expected

def test_drop_temporary_outdated_biosphere_flows():
    given = [{
      'exchanges': [{
        'flow': 'dd80f0f2-f4d5-40f0-9035-09c1a7f3f07b',
        'type': 'production',
        'name': 'heat, central or small-scale, other than natural gas',
        'unit': 'megajoule',
        'amount': 1.0,
      }, {
        'flow': '1fa64d0c-afd7-46ab-b95c-3a54e0902dd0',
        'type': 'technosphere',
        'name': 'coke',
        'unit': 'megajoule',
        'amount': 1.43,
      }, {
        'flow': '6edcc2df-88a3-48e1-83d8-ffc38d31c35b',
        'type': 'biosphere',
        'name': 'Carbon monoxide, fossil',
        'unit': 'kilogram',
        'amount': 0.00715,
      }, {
        'flow': '855a44a3-558b-485b-8358-3bc84fd83da8',
        'type': 'biosphere',
        'name': 'Fluorene_temp',
        'unit': 'kilogram',
        'amount': 6.5430904e-13,
      }, {
        'flow': 'cacad1fe-ccbe-4e32-80fa-37afc755156b',
        'type': 'biosphere',
        'name': 'Fluoranthene_temp',
        'unit': 'kilogram',
        'amount': 7.2054278e-13,
      }]
    }]

    expected = [{
      'exchanges': [{
        'flow': 'dd80f0f2-f4d5-40f0-9035-09c1a7f3f07b',
        'type': 'production',
        'name': 'heat, central or small-scale, other than natural gas',
        'unit': 'megajoule',
        'amount': 1.0,
      }, {
        'flow': '1fa64d0c-afd7-46ab-b95c-3a54e0902dd0',
        'type': 'technosphere',
        'name': 'coke',
        'unit': 'megajoule',
        'amount': 1.43,
      }, {
        'flow': '6edcc2df-88a3-48e1-83d8-ffc38d31c35b',
        'type': 'biosphere',
        'name': 'Carbon monoxide, fossil',
        'unit': 'kilogram',
        'amount': 0.00715,
      }]
    }]

    assert(drop_temporary_outdated_biosphere_flows(given)) == expected
