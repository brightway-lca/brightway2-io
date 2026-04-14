import math
import re

import numpy as np

from ..errors import StrategyError


def csv_restore_tuples(data):
    """
    Convert tuple-like strings to actual tuples.

    Parameters
    ----------
    data : list of dict
        A list of datasets.

    Returns
    -------
    list of dict
        A list of datasets with tuples restored from string.

    Examples
    --------
    >>> data = [{'categories': 'category1::category2'}, {'exchanges': [{'categories': 'category3::category4', 'amount': '10.0'}]}]
    >>> csv_restore_tuples(data)
    [{'categories': ('category1', 'category2')}, {'exchanges': [{'categories': ('category3', 'category4'), 'amount': '10.0'}]}]

    """
    _ = lambda x: tuple(x.split("::")) if "::" in x else x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
            if key == "categories" and isinstance(ds[key], str):
                ds[key] = (ds[key],)
        for exc in ds.get("exchanges", []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
                if key == "categories" and isinstance(exc[key], str):
                    exc[key] = (exc[key],)
    return data


def csv_restore_booleans(data):
    """
    Convert boolean-like strings to booleans where possible.

    Parameters
    ----------
    data : list of dict
        A list of datasets.

    Returns
    -------
    list of dict
        A list of datasets with booleans restored.

    Examples
    --------
    >>> data = [{'categories': 'category1', 'is_animal': 'true'}, {'exchanges': [{'categories': 'category2', 'amount': '10.0', 'uncertainty type': 'undefined', 'is_biomass': 'False'}]}]
    >>> csv_restore_booleans(data)
    [{'categories': 'category1', 'is_animal': True}, {'exchanges': [{'categories': 'category2', 'amount': '10.0', 'uncertainty type': 'undefined', 'is_biomass': False}]}]
    """

    def _(x):
        if x.lower() == "true":
            return True
        elif x.lower() == "false":
            return False
        else:
            return x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
        for exc in ds.get("exchanges", []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
    return data


def csv_numerize(data):
    """
    Convert string values to float or int where possible

    Parameters
    ----------
    data : list of dict
        A list of datasets.

    Returns
    -------
    list of dict
        A list of datasets with string values converted to float or int where possible.

    Examples
    --------
    >>> data = [{'amount': '10.0'}, {'exchanges': [{'amount': '20', 'uncertainty type': 'undefined'}]}]
    >>> csv_numerize(data)
    [{'amount': 10.0}, {'exchanges': [{'amount': 20, 'uncertainty type': 'undefined'}]}]
    """

    def _(x):
        try:
            return float(x)
        except:
            return x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
        for exc in ds.get("exchanges", []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
    return data


def csv_drop_unknown(data):
    """
    Remove any keys whose values are `(Unknown)`.

    Parameters
    ----------
    data : list[dict]
        A list of dictionaries, where each dictionary represents a row of data.

    Returns
    -------
    list[dict]
        The updated list of dictionaries with `(Unknown)` values removed from the keys.

    Examples
    --------
    >>> data = [
            {"name": "John", "age": 30, "gender": "(Unknown)"},
            {"name": "Alice", "age": 25, "gender": "Female"},
            {"name": "Bob", "age": 40, "gender": "Male"}
        ]
    >>> csv_drop_unknown(data)
        [
            {"name": "Alice", "age": 25, "gender": "Female"},
            {"name": "Bob", "age": 40, "gender": "Male"}
        ]
    """
    _ = lambda x: None if x == "(Unknown)" else x

    data = [{k: v for k, v in ds.items() if v != "(Unknown)"} for ds in data]

    for ds in data:
        if "exchanges" in ds:
            ds["exchanges"] = [
                {k: v for k, v in exc.items() if v != "(Unknown)"}
                for exc in ds["exchanges"]
            ]

    return data


def csv_add_missing_exchanges_section(data):
    """
    Add an empty `exchanges` section to any dictionary in `data` that doesn't already have one.

    Parameters
    ----------
    data: list of dict
        A list of dictionaries, where each dictionary represents a row of data.

    Returns
    -------
    list[dict]
        The updated list of dictionaries with an empty `exchanges` section added to any dictionary that doesn't already have one.

    Examples
    --------
    >>> data = [
            {"name": "John", "age": 30},
            {"name": "Alice", "age": 25, "exchanges": []},
            {"name": "Bob", "age": 40, "exchanges": [{"name": "NYSE"}]}
        ]
    >>> csv_add_missing_exchanges_section(data)
        [
            {"name": "John", "age": 30, "exchanges": []},
            {"name": "Alice", "age": 25, "exchanges": []},
            {"name": "Bob", "age": 40, "exchanges": [{"name": "NYSE"}]}
        ]
    """
    for ds in data:
        if "exchanges" not in ds:
            ds["exchanges"] = []
    return data


def _is_blank(value):
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return True
    return False


def _normalize_kind(value):
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    if value in {"delta", "relative", "timedelta64"}:
        return "delta"
    if value in {"abs", "absolute", "datetime64"}:
        return "abs"
    if value in {"easy_timedelta_distribution", "easy_timedelta", "easy_td"}:
        return "easy_timedelta_distribution"
    if value in {"easy_datetime_distribution", "easy_datetime", "easy_dt"}:
        return "easy_datetime_distribution"
    return None


def _normalize_resolution(value, exc):
    if not isinstance(value, str):
        raise StrategyError(
            "Invalid resolution '{}' in exchange {}".format(
                value, exc.get("name", "<unknown>")
            )
        )
    value = value.strip()
    if not value:
        raise StrategyError(
            "Empty resolution in exchange {}".format(exc.get("name", "<unknown>"))
        )
    if len(value) == 1:
        lower = value.lower()
        if lower == "y":
            return "Y"
        if lower == "d":
            return "D"
        if lower == "h":
            return "h"
        if lower == "s":
            return "s"
        if lower == "m":
            # Lowercase 'm' → minutes; uppercase 'M' → months
            return "m" if value == "m" else "M"
    return value


def _parse_sequence(value, field, exc):
    if isinstance(value, (list, tuple)):
        seq = list(value)
    elif isinstance(value, str):
        if "," in value:
            seq = [s.strip() for s in value.split(",") if s.strip() != ""]
        elif "::" in value:
            seq = [s.strip() for s in value.split("::") if s.strip() != ""]
        else:
            seq = [value.strip()]
    else:
        seq = [value]
    if not seq:
        raise StrategyError(
            "Field '{}' is empty in exchange {}".format(
                field, exc.get("name", "<unknown>")
            )
        )
    return seq


def _coerce_int_list(seq, field, exc):
    values = []
    for item in seq:
        if isinstance(item, bool):
            raise StrategyError(
                "Invalid integer value '{}' in field '{}' for exchange {}".format(
                    item, field, exc.get("name", "<unknown>")
                )
            )
        if isinstance(item, (int, np.integer)):
            values.append(int(item))
            continue
        if isinstance(item, float):
            if item.is_integer():
                values.append(int(item))
                continue
            raise StrategyError(
                "Invalid integer value '{}' in field '{}' for exchange {}".format(
                    item, field, exc.get("name", "<unknown>")
                )
            )
        if isinstance(item, str):
            if re.match(r"^-?\d+$", item.strip()):
                values.append(int(item.strip()))
                continue
        raise StrategyError(
            "Invalid integer value '{}' in field '{}' for exchange {}".format(
                item, field, exc.get("name", "<unknown>")
            )
        )
    return values


def _coerce_float_list(seq, field, exc):
    values = []
    for item in seq:
        try:
            values.append(float(item))
        except (ValueError, TypeError):
            raise StrategyError(
                "Invalid float value '{}' in field '{}' for exchange {}".format(
                    item, field, exc.get("name", "<unknown>")
                )
            )
    return values


def _normalize_abs_dates(seq, resolution, exc):
    resolution = resolution.strip()
    if resolution == "Y":
        normalized = []
        for item in seq:
            if isinstance(item, bool):
                raise StrategyError(
                    "Invalid year value '{}' in exchange {}".format(
                        item, exc.get("name", "<unknown>")
                    )
                )
            if isinstance(item, (int, np.integer)):
                year = str(int(item))
            elif isinstance(item, float) and item.is_integer():
                year = str(int(item))
            elif isinstance(item, str):
                year = item.strip()
            else:
                raise StrategyError(
                    "Invalid year value '{}' in exchange {}".format(
                        item, exc.get("name", "<unknown>")
                    )
                )
            if not re.match(r"^\d{4}$", year):
                raise StrategyError(
                    "Year value '{}' does not match YYYY format in exchange {}".format(
                        year, exc.get("name", "<unknown>")
                    )
                )
            normalized.append(year)
        return normalized
    if resolution == "M":
        normalized = []
        for item in seq:
            if not isinstance(item, str):
                raise StrategyError(
                    "Month value '{}' must be a string in exchange {}".format(
                        item, exc.get("name", "<unknown>")
                    )
                )
            item = item.strip()
            match = re.match(r"^(0?[1-9]|1[0-2])-(\d{4})$", item)
            if not match:
                raise StrategyError(
                    "Month value '{}' does not match M-YYYY format in exchange {}".format(
                        item, exc.get("name", "<unknown>")
                    )
                )
            month = int(match.group(1))
            year = match.group(2)
            normalized.append("{}-{:02d}".format(year, month))
        return normalized
    if resolution == "D":
        normalized = []
        for item in seq:
            if not isinstance(item, str):
                raise StrategyError(
                    "Day value '{}' must be a string in exchange {}".format(
                        item, exc.get("name", "<unknown>")
                    )
                )
            item = item.strip()
            match = re.match(r"^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-(\d{4})$", item)
            if not match:
                raise StrategyError(
                    "Day value '{}' does not match D-M-YYYY format in exchange {}".format(
                        item, exc.get("name", "<unknown>")
                    )
                )
            day = int(match.group(1))
            month = int(match.group(2))
            year = match.group(3)
            normalized.append("{}-{:02d}-{:02d}".format(year, month, day))
        return normalized

    # For other resolutions (h, m, s, etc.), require ISO-formatted strings
    normalized = []
    for item in seq:
        if not isinstance(item, str):
            raise StrategyError(
                "Date value '{}' must be a string for resolution '{}' in exchange {}".format(
                    item, resolution, exc.get("name", "<unknown>")
                )
            )
        normalized.append(item.strip())
    return normalized


def csv_restore_temporal_distributions(data):
    """
    Reconstruct TemporalDistribution objects from exchange row columns.

    Accepts both ``temporal_distribution`` (underscore) and
    ``temporal distribution`` (space) as the key name; the underscore form
    takes precedence when both are present.

    Expected exchange fields:

    - ``temporal_distribution``: one of ``delta`` / ``relative`` /
      ``timedelta64``, ``abs`` / ``absolute`` / ``datetime64``,
      ``easy_timedelta_distribution`` (aliases: ``easy_timedelta``,
      ``easy_td``), or ``easy_datetime_distribution`` (aliases:
      ``easy_datetime``, ``easy_dt``).
    - ``date``: list/tuple or comma-separated string of offsets (delta) or
      formatted date strings (abs).
    - ``value``: list/tuple or comma-separated string of floats.  Rescaled to
      sum to 1 if necessary.
    - ``resolution``: numpy time-unit code such as ``Y``, ``M``, ``D``,
      ``h``, ``m``, ``s``.  Case is significant for ``m`` (minutes) vs
      ``M`` (months); all other single-letter codes are case-insensitive.
    - For ``easy_timedelta_distribution``: also ``start``, ``end``,
      ``steps``, ``resolution``; and optionally ``td_kind``, ``td_param``.
    - For ``easy_datetime_distribution``: also ``start``, ``end``, ``steps``.

    Raises
    ------
    StrategyError
        On any validation failure (unknown kind, missing fields, bad values,
        mismatched lengths, zero-sum amounts).
    StrategyError
        If ``bw_temporalis`` is not installed and a temporal distribution is
        encountered.
    """
    try:
        from bw_temporalis import (
            TemporalDistribution,
            easy_datetime_distribution,
            easy_timedelta_distribution,
        )
    except Exception:
        TemporalDistribution = None
        easy_timedelta_distribution = None
        easy_datetime_distribution = None

    for ds in data:
        for exc in ds.get("exchanges", []):
            # Normalise the space-variant key; underscore form wins if both present.
            if "temporal distribution" in exc:
                if "temporal_distribution" not in exc:
                    exc["temporal_distribution"] = exc.pop("temporal distribution")
                else:
                    del exc["temporal distribution"]

            kind = _normalize_kind(exc.get("temporal_distribution"))
            if not kind:
                if not _is_blank(exc.get("temporal_distribution")):
                    raise StrategyError(
                        "Unknown temporal_distribution value '{}' in exchange {}".format(
                            exc.get("temporal_distribution"),
                            exc.get("name", "<unknown>"),
                        )
                    )
                continue

            if TemporalDistribution is None:
                raise StrategyError(
                    "Temporal distributions require `bw_temporalis` to be installed"
                )

            if kind == "easy_timedelta_distribution":
                required = ("start", "end", "steps", "td_kind", "td_param", "resolution")
            elif kind == "easy_datetime_distribution":
                required = ("start", "end", "steps")
            else:
                required = ("date", "value", "resolution")

            missing = [k for k in required if k not in exc]
            if missing:
                raise StrategyError(
                    "Missing required temporal distribution fields {} in exchange {}".format(
                        missing, exc.get("name", "<unknown>")
                    )
                )

            if kind == "easy_timedelta_distribution":
                resolution = _normalize_resolution(exc.get("resolution"), exc)
                try:
                    start = int(exc.get("start"))
                    end = int(exc.get("end"))
                    steps = int(exc.get("steps"))
                except (ValueError, TypeError):
                    raise StrategyError(
                        "Invalid start/end/steps values in exchange {}".format(
                            exc.get("name", "<unknown>")
                        )
                    )
                td_kind = exc.get("td_kind")
                td_param = exc.get("td_param")
                if _is_blank(td_kind):
                    td_kind = "uniform"
                if _is_blank(td_param):
                    td_param = None
                else:
                    try:
                        td_param = float(td_param)
                    except (ValueError, TypeError):
                        raise StrategyError(
                            "Invalid td_param '{}' in exchange {}".format(
                                td_param, exc.get("name", "<unknown>")
                            )
                        )
                try:
                    td_obj = easy_timedelta_distribution(
                        start=start,
                        end=end,
                        resolution=resolution,
                        steps=steps,
                        kind=str(td_kind),
                        param=td_param,
                    )
                except Exception as exc_err:
                    raise StrategyError(
                        "Failed to build easy_timedelta_distribution in exchange {}: {}".format(
                            exc.get("name", "<unknown>"), exc_err
                        )
                    ) from exc_err

                exc["temporal_distribution_kind"] = exc.get("temporal_distribution")
                exc["temporal_distribution"] = td_obj
                for key in ("start", "end", "steps", "td_kind", "td_param", "resolution"):
                    exc.pop(key, None)

            elif kind == "easy_datetime_distribution":
                try:
                    start = str(exc.get("start"))
                    end = str(exc.get("end"))
                    steps = int(exc.get("steps"))
                except (ValueError, TypeError):
                    raise StrategyError(
                        "Invalid start/end/steps values in exchange {}".format(
                            exc.get("name", "<unknown>")
                        )
                    )
                if _is_blank(start) or _is_blank(end):
                    raise StrategyError(
                        "Missing start/end in exchange {}".format(
                            exc.get("name", "<unknown>")
                        )
                    )
                try:
                    td_obj = easy_datetime_distribution(start=start, end=end, steps=steps)
                except Exception as exc_err:
                    raise StrategyError(
                        "Failed to build easy_datetime_distribution in exchange {}: {}".format(
                            exc.get("name", "<unknown>"), exc_err
                        )
                    ) from exc_err

                exc["temporal_distribution_kind"] = exc.get("temporal_distribution")
                exc["temporal_distribution"] = td_obj
                for key in ("start", "end", "steps"):
                    exc.pop(key, None)

            else:  # delta or abs
                resolution = _normalize_resolution(exc.get("resolution"), exc)

                dates_raw = _parse_sequence(exc["date"], "date", exc)
                amounts_raw = _parse_sequence(exc["value"], "value", exc)

                if len(dates_raw) != len(amounts_raw):
                    raise StrategyError(
                        "Mismatched date/value lengths in exchange {}".format(
                            exc.get("name", "<unknown>")
                        )
                    )

                if kind == "delta":
                    date_values = _coerce_int_list(dates_raw, "date", exc)
                    try:
                        date_array = np.array(
                            date_values, dtype="timedelta64[{}]".format(resolution)
                        )
                    except Exception as exc_err:
                        raise StrategyError(
                            "Invalid timedelta resolution '{}' in exchange {}".format(
                                resolution, exc.get("name", "<unknown>")
                            )
                        ) from exc_err
                else:
                    date_values = _normalize_abs_dates(dates_raw, resolution, exc)
                    try:
                        date_array = np.array(
                            date_values, dtype="datetime64[{}]".format(resolution)
                        )
                    except Exception as exc_err:
                        raise StrategyError(
                            "Invalid date values for resolution '{}' in exchange {}".format(
                                resolution, exc.get("name", "<unknown>")
                            )
                        ) from exc_err

                amount_values = _coerce_float_list(amounts_raw, "value", exc)
                total = sum(amount_values)
                if total == 0:
                    raise StrategyError(
                        "Temporal distribution amounts sum to zero in exchange {}".format(
                            exc.get("name", "<unknown>")
                        )
                    )
                if not math.isclose(total, 1.0, rel_tol=1e-9, abs_tol=1e-12):
                    amount_values = [a / total for a in amount_values]
                amount_array = np.array(amount_values, dtype=float)

                exc["temporal_distribution_kind"] = exc.get("temporal_distribution")
                exc["temporal_distribution"] = TemporalDistribution(
                    date_array, amount_array
                )
                exc.pop("date", None)
                exc.pop("value", None)
                exc.pop("resolution", None)

    return data
