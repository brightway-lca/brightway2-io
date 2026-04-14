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
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)
    assert "date" not in exc
    assert "amount" not in exc
    assert "resolution" not in exc

    td = exc["temporal_distribution"]
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
    td_year = exc_year["temporal_distribution"]
    assert isinstance(td_year, TemporalDistribution)
    assert td_year.date[0] == np.datetime64("2025-01-01")

    exc_month = data[0]["exchanges"][1]
    td_month = exc_month["temporal_distribution"]
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
    assert np.isclose(exc_delta["temporal_distribution"].amount.sum(), 1.0)
    assert np.allclose(exc_delta["temporal_distribution"].amount, np.array([1.0]))

    exc_abs = data[0]["exchanges"][1]
    assert np.isclose(exc_abs["temporal_distribution"].amount.sum(), 1.0)
    assert np.allclose(exc_abs["temporal_distribution"].amount, np.array([1.0]))


def test_csv_restore_temporal_distributions_skips_when_type_blank():
    data = [
        {
            "exchanges": [
                {
                    "name": "blank",
                    "temporal_distribution": "",
                    "date": "",
                    "value": "",
                    "resolution": "",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert exc.get("temporal_distribution") == ""


def test_csv_restore_temporal_distributions_blanks_with_type_raise():
    data = [
        {
            "exchanges": [
                {
                    "name": "bad",
                    "temporal_distribution": "delta",
                    "date": "",
                    "value": "",
                    "resolution": "",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)


def test_csv_restore_temporal_distributions_unknown_kind_raises():
    data = [
        {
            "exchanges": [
                {
                    "name": "bad-kind",
                    "temporal_distribution": "detla",
                    "date": "-1",
                    "value": "1",
                    "resolution": "Y",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)


def test_csv_restore_temporal_distributions_easy_timedelta_distribution():
    data = [
        {
            "exchanges": [
                {
                    "name": "easy-td",
                    "temporal_distribution": "easy_timedelta_distribution",
                    "start": 0,
                    "end": 10,
                    "steps": 5,
                    "td_kind": "triangular",
                    "td_param": 3,
                    "resolution": "h",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)
    assert exc.get("temporal_distribution_kind") == "easy_timedelta_distribution"


def test_csv_restore_temporal_distributions_easy_timedelta_alias():
    data = [
        {
            "exchanges": [
                {
                    "name": "easy-td",
                    "temporal_distribution": "easy_td",
                    "start": 0,
                    "end": 10,
                    "steps": 5,
                    "td_kind": "triangular",
                    "td_param": 3,
                    "resolution": "h",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)
    assert exc.get("temporal_distribution_kind") == "easy_td"


def test_csv_restore_temporal_distributions_easy_timedelta_defaults():
    data = [
        {
            "exchanges": [
                {
                    "name": "easy-td-defaults",
                    "temporal_distribution": "easy_timedelta",
                    "start": 0,
                    "end": 10,
                    "steps": 5,
                    "resolution": "h",
                    "td_kind": "",
                    "td_param": "",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)


def test_csv_restore_temporal_distributions_easy_datetime_distribution():
    data = [
        {
            "exchanges": [
                {
                    "name": "easy-dt",
                    "temporal_distribution": "easy_datetime_distribution",
                    "start": "2023-05-23",
                    "end": "2023-05-24",
                    "steps": 10,
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)
    assert exc.get("temporal_distribution_kind") == "easy_datetime_distribution"


def test_csv_restore_temporal_distributions_easy_datetime_alias():
    data = [
        {
            "exchanges": [
                {
                    "name": "easy-dt",
                    "temporal_distribution": "easy_dt",
                    "start": "2023-05-23",
                    "end": "2023-05-24",
                    "steps": 10,
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)
    assert exc.get("temporal_distribution_kind") == "easy_dt"


def test_csv_restore_temporal_distributions_lowercase_resolution():
    data = [
        {
            "exchanges": [
                {
                    "name": "lowercase-res",
                    "temporal_distribution": "delta",
                    "date": "1,2",
                    "value": "0.5,0.5",
                    "resolution": "y",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)


def test_csv_restore_temporal_distributions_mismatched_lengths_raises():
    data = [
        {
            "exchanges": [
                {
                    "name": "mismatch",
                    "temporal_distribution": "delta",
                    "date": "1,2,3",
                    "value": "0.5,0.5",
                    "resolution": "Y",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)


def test_csv_restore_temporal_distributions_zero_sum_raises():
    data = [
        {
            "exchanges": [
                {
                    "name": "zero-sum",
                    "temporal_distribution": "delta",
                    "date": "1,2",
                    "value": "0,0",
                    "resolution": "Y",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)


def test_csv_restore_temporal_distributions_space_key():
    """'temporal distribution' (space) should be accepted as an alias."""
    data = [
        {
            "exchanges": [
                {
                    "name": "space-key",
                    "temporal distribution": "delta",
                    "date": "1,2",
                    "value": "0.4,0.6",
                    "resolution": "Y",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    assert isinstance(exc["temporal_distribution"], TemporalDistribution)
    assert "temporal distribution" not in exc


def test_csv_restore_temporal_distributions_abs_day():
    data = [
        {
            "exchanges": [
                {
                    "name": "day",
                    "temporal_distribution": "abs",
                    "date": "15-10-2024,1-3-2025",
                    "value": "0.6,0.4",
                    "resolution": "D",
                }
            ]
        }
    ]

    csv_restore_temporal_distributions(data)
    exc = data[0]["exchanges"][0]
    td = exc["temporal_distribution"]
    assert isinstance(td, TemporalDistribution)
    assert td.date[0] == np.datetime64("2024-10-15")
    assert td.date[1] == np.datetime64("2025-03-01")


def test_csv_restore_temporal_distributions_easy_timedelta_missing_field_raises():
    data = [
        {
            "exchanges": [
                {
                    "name": "missing-field",
                    "temporal_distribution": "easy_timedelta",
                    "start": 0,
                    "end": 10,
                    # steps is missing
                    "td_kind": "uniform",
                    "td_param": "",
                    "resolution": "h",
                }
            ]
        }
    ]

    with pytest.raises(StrategyError):
        csv_restore_temporal_distributions(data)
