from bw2io.strategies.ecospold2 import (
    fix_unreasonably_high_lognormal_uncertainties,
    remove_uncertainty_from_negative_loss_exchanges,
    set_lognormal_loc_value,
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
