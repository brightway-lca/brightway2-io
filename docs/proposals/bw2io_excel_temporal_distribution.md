# Proposal: Excel/CSV temporal distributions on exchanges

## Goal
Enable the Excel/CSV LCI importer to recognize exchange-row columns that describe a temporal distribution and reconstruct a `bw_temporalis.TemporalDistribution` from those columns (in addition to any existing JSON-based handling in downstream code). This should be done without changing runtime behavior elsewhere or requiring in-place edits to installed packages.

## Best insertion point
The most stable and minimal hook is the **Excel/CSV strategy pipeline** used by `ExcelImporter` and `CSVImporter`:

- `bw2io/importers/excel.py` defines the strategy list that transforms exchanges after extraction.
- `bw2io/strategies/csv.py` already contains Excel/CSV helpers (`csv_restore_tuples`, `csv_numerize`, etc.).

A new strategy function (e.g., `csv_restore_temporal_distributions`) should live in `bw2io/strategies/csv.py` and be inserted in the `ExcelImporter.strategies` list **after** `csv_restore_tuples`/`csv_restore_booleans`/`csv_numerize`/`csv_drop_unknown`, and **before** any linking or normalization. This ensures we can parse tuple-like cell values and have cleaned inputs before reconstructing the temporal distribution object.

## Column spec (exchange rows)
The importer should interpret the following exchange-level columns:

- `temporal_distribution`: string flag, case-insensitive (only these values are accepted)
  - `delta`, `relative`, or `timedelta64` → `timedelta64[resolution]`
  - `abs`, `absolute`, or `datetime64` → `datetime64[resolution]`
- `date`: list/tuple **or comma-separated string** of integer offsets for `delta` (e.g., `-3,-2,1,3`) or **formatted date strings for `abs`** matching `resolution` (e.g., `2025,2026,2030` for `Y`; `10-2024,5-2025,8-2025` for `M`; `15-10-2024` for `D`), and same length as `amount`
- `amount`: list/tuple of floats (same length as `date`) that sum to 1.0; if not, rescale by dividing by their sum
- `resolution`: time resolution string such as `Y`, `M`, `D`, etc.

If `temporal_distribution` is present but does not match any recognized value, the strategy should **not** attempt reconstruction and should leave the exchange untouched.

## Proposed strategy behavior (high-level)
Add a helper in `bw2io/strategies/csv.py`:

- `csv_restore_temporal_distributions(data)`
  - Iterate datasets and exchanges.
  - For each exchange with `temporal_distribution` in `{delta, abs}`:
    - Validate required keys: `date`, `amount`, `resolution`.
    - Accept `date`/`amount` as tuples, lists, or comma-separated strings (post `csv_restore_tuples`).
    - For `delta`, require integer offsets in `date` (e.g., `-3,-2,1,3`).
    - For `abs`, require date strings that match `resolution` (e.g., `YYYY` for `Y`, `M-YYYY` for `M`, `D-M-YYYY` for `D`) and normalize to ISO strings before building the numpy array.
    - Ensure `len(date) == len(amount)`.
    - Require `amount` to be floats and rescale if their sum is not 1.0 (divide by sum).
    - Build `numpy.ndarray` with dtype:
      - `timedelta64[resolution]` for `delta`
      - `datetime64[resolution]` for `abs`
    - Construct a `bw_temporalis.TemporalDistribution` from `date` and `amount`.
    - Store it under a canonical key (see below).

### Canonical storage key
Use `exc["temporal distribution"]` (note the space) for the constructed object, to match common Brightway conventions. Retain the original `temporal_distribution` column as a flag for provenance, and **remove** `date`/`amount`/`resolution` after successful reconstruction to reduce downstream ambiguity.

## Expected code touch points (minimal)
- `bw2io/strategies/csv.py`
  - Add `csv_restore_temporal_distributions` and any small helper (e.g., `_build_temporal_distribution`).
- `bw2io/strategies/__init__.py`
  - Export the new strategy so it can be imported by the importer.
- `bw2io/importers/excel.py`
  - Insert the new strategy into `ExcelImporter.strategies` (applies to `CSVImporter` as well).

No changes are required in `bw2io/importers/base_lci.py` because the data flow for Excel/CSV exchanges is already mediated by the strategy pipeline.

## Example spreadsheet row
```
name, temporal_distribution, date, amount, resolution
"Electricity", delta, "-3,-2,1,3", "0.1,0.2,0.5,0.2", M
```

## Edge cases and validations
- Missing any required columns should raise a `StrategyError` with a clear message and row context.
- If `date`/`amount` are not list/tuple-like after `csv_restore_tuples`, the strategy should fail fast with a `StrategyError`.
- If `resolution` is invalid for numpy dtypes, allow the exception to surface but wrap it to include the exchange name.
- If a scalar exchange `amount` is also needed, define a distinct column like `exchange_amount` in a follow-on change and map it separately to avoid ambiguity.

## Open questions
1. Should the strategy silently skip invalid `temporal_distribution` values or raise an error?
