import pytest
from bw2io.strategies.sentier import match_internal_simapro_simapro_with_unit_conversion, SimaProUnitConverter


def test_integration_SPUC():
    spuc = SimaProUnitConverter()
    result = spuc.get_simapro_conversions('MJ')
    expected = [
        {'qk': 'https://vocab.sentier.dev/qudt/quantity-kind/Energy',
        'factor': 0.001,
        'unit': 'GJ'},
        {'qk': 'https://vocab.sentier.dev/qudt/quantity-kind/Energy',
        'factor': 1000.0,
        'unit': 'kJ'},
        {'qk': 'https://vocab.sentier.dev/qudt/quantity-kind/Energy',
        'factor': 0.2777777777777778,
        'unit': 'kWh'},
        {'qk': 'https://vocab.sentier.dev/qudt/quantity-kind/Energy',
        'factor': 0.0002777777777777778,
        'unit': 'MWh'},
        {'qk': 'https://vocab.sentier.dev/qudt/quantity-kind/Energy',
        'factor': 277.77777777777777,
        'unit': 'Wh'}
    ]
    for obj in expected:
        assert obj in result

    result = spuc.get_simapro_conversions('km')
    expected = [
        {'qk': 'https://vocab.sentier.dev/qudt/quantity-kind/Length',
        'factor': 1000.0,
        'unit': 'm'}
    ]
    for obj in expected:
        assert obj in result
