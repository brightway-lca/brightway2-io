import pytest

from bw2io.strategies.sentier import (
    SimaProUnitConverter,
    match_internal_simapro_simapro_with_unit_conversion,
)


def test_integration_SPUC():
    spuc = SimaProUnitConverter()
    result = spuc.get_simapro_conversions("MJ")
    expected = [
        {
            "qk": "https://vocab.sentier.dev/units/quantity-kind/Energy",
            "factor": 0.001,
            "unit": "GJ",
        },
        {
            "qk": "https://vocab.sentier.dev/units/quantity-kind/Energy",
            "factor": 1000.0,
            "unit": "kJ",
        },
        {
            "qk": "https://vocab.sentier.dev/units/quantity-kind/Energy",
            "factor": 0.2777777777777778,
            "unit": "kWh",
        },
        {
            "qk": "https://vocab.sentier.dev/units/quantity-kind/Energy",
            "factor": 0.0002777777777777778,
            "unit": "MWh",
        },
        {
            "qk": "https://vocab.sentier.dev/units/quantity-kind/Energy",
            "factor": 277.77777777777777,
            "unit": "Wh",
        },
    ]
    print(result)
    for obj in expected:
        assert obj in result

    result = spuc.get_simapro_conversions("km")
    expected = [
        {
            "qk": "https://vocab.sentier.dev/units/quantity-kind/Length",
            "factor": 1000.0,
            "unit": "m",
        }
    ]
    for obj in expected:
        assert obj in result


def test_match_internal_simapro_simapro_with_unit_conversion():
    given = [
        {
            "name": "foo",
            "location": "bar",
            "database": "a",
            "code": "b",
            "unit": "km",
            "exchanges": [
                {"name": "foo", "location": "bar", "unit": "m", "amount": 1000.0}
            ],
        }
    ]
    expected = [
        {
            "name": "foo",
            "location": "bar",
            "database": "a",
            "code": "b",
            "unit": "km",
            "exchanges": [
                {
                    "input": ("a", "b"),
                    "name": "foo",
                    "location": "bar",
                    "unit": "km",
                    "loc": 1.0,
                    "amount": 1.0,
                }
            ],
        }
    ]
    assert match_internal_simapro_simapro_with_unit_conversion(given) == expected
