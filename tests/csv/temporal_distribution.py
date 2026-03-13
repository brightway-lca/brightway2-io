import numpy as np
import pytest

from bw2io.errors import StrategyError
from bw2io.strategies.csv import csv_restore_temporal_distributions

pytest.importorskip("bw_temporalis")
from bw_temporalis import TemporalDistribution


def test_csv_restore_temporal_distributions_delta_rescales():
    data = [
        {
            "exchanges": [
                {
                    "name": "foo",
                    "temporal_distribution": "delta",
                    "date": "-3,-2,1,3",
                    "value": "1,1,1,1",
                    "resolution": "M",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)

    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal distribution"], TemporalDistribution)
    assert "date" not in exc
    assert "amount" not in exc
    assert "resolution" not in exc

    td = exc["temporal distribution"]
    assert np.isclose(td.amount.sum(), 1.0)
    assert np.allclose(td.amount, np.array([0.25, 0.25, 0.25, 0.25]))


def test_csv_restore_temporal_distributions_abs_year_and_month():
    data = [
        {
            "exchanges": [
                {
                    "name": "year",
                    "temporal_distribution": "absolute",
                    "date": (2025, 2026),
                    "value": (0.4, 0.6),
                    "resolution": "Y",
                },
                {
                    "name": "month",
                    "temporal_distribution": "datetime64",
                    "date": "10-2024,5-2025,8-2025",
                    "value": "0.2,0.3,0.5",
                    "resolution": "M",
                },
            ]
        }
    ]

    csv_restore_temporal_distributions(data)

    exc_year = data[0]["exchanges"][0]
    td_year = exc_year["temporal distribution"]
    assert isinstance(td_year, TemporalDistribution)
    assert td_year.date[0] == np.datetime64("2025-01-01")

    exc_month = data[0]["exchanges"][1]
    td_month = exc_month["temporal distribution"]
    assert isinstance(td_month, TemporalDistribution)
    assert td_month.date[0] == np.datetime64("2024-10-01")


def test_csv_restore_temporal_distributions_invalid_month_format_raises():
    data = [
        {
            "exchanges": [
                {
                    "name": "bad",
                    "temporal_distribution": "abs",
                    "date": "2024-10",
                    "value": "1",
                    "resolution": "M",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)


def test_csv_restore_temporal_distributions_invalid_delta_integer_raises():
    data = [
        {
            "exchanges": [
                {
                    "name": "bad",
                    "temporal_distribution": "relative",
                    "date": "1.5,2",
                    "value": "0.5,0.5",
                    "resolution": "D",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)


def test_csv_restore_temporal_distributions_accepts_single_values():
    data = [
        {
            "exchanges": [
                {
                    "name": "single-delta",
                    "temporal_distribution": "timedelta64",
                    "date": -2,
                    "value": 2,
                    "resolution": "D",
                },
                {
                    "name": "single-abs",
                    "temporal_distribution": "datetime64",
                    "date": "2025",
                    "value": "2.5",
                    "resolution": "Y",
                },
            ]
        }
    ]

    csv_restore_temporal_distributions(data)

    exc_delta = data[0]["exchanges"][0]
    assert np.isclose(exc_delta["temporal distribution"].amount.sum(), 1.0)
    assert np.allclose(exc_delta["temporal distribution"].amount, np.array([1.0]))

    exc_abs = data[0]["exchanges"][1]
    assert np.isclose(exc_abs["temporal distribution"].amount.sum(), 1.0)
    assert np.allclose(exc_abs["temporal distribution"].amount, np.array([1.0]))
