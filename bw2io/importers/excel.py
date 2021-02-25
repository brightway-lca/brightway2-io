from ..extractors import ExcelExtractor, CSVExtractor
from ..strategies import (
    add_database_name,
    assign_only_product_as_production,
    convert_uncertainty_types_to_integers,
    csv_drop_unknown,
    csv_numerize,
    csv_restore_booleans,
    csv_restore_tuples,
    csv_add_missing_exchanges_section,
    drop_falsey_uncertainty_fields_but_keep_zeros,
    link_iterable_by_fields,
    link_technosphere_by_activity_hash,
    normalize_biosphere_categories,
    normalize_biosphere_names,
    normalize_units,
    set_code_by_activity_hash,
    strip_biosphere_exc_locations,
    convert_activity_parameters_to_list,
)
from .base_lci import LCIImporter
from bw2data import Database, config
from time import time
import functools
import warnings


is_empty_line = lambda line: not line or not any(line)
remove_empty = lambda dct: {k: v for k, v in dct.items() if (v or v == 0)}


def valid_first_cell(sheet, data):
    """Return boolean if first cell in worksheet is not ``skip``."""
    try:
        return hasattr(data[0][0], "lower") and data[0][0].lower() != "skip"
    except:
        warnings.warn("Invalid first cell (A1) in worksheet {}".format(sheet))
        return False


class ExcelImporter(LCIImporter):
    """Generic Excel importer.

    See the `generic Excel example spreadsheet <https://github.com/brightway-lca/brightway2-io/raw/master/bw2io/data/examples/example.xlsx>`__.

    Excel spreadsheet should follow the following format:

    ::
        Project parameters
        <variable>, <formula>, <amount>, metadata

        Database, <name of database>
        <database field name>, <database field value>

        Parameters
        <variable>, <formula>, <amount>, metadata

        Activity, <name of activity>
        <database field name>, <database field value>
        Exchanges
        <field name>, <field name>, <field name>
        <value>, <value>, <value>
        <value>, <value>, <value>

    Neither project parameters, parameters, nor exchanges for each activity are required.

    An activity is marked as finished with a blank line.

    In general, data is imported without modification. However, the following transformations are applied:

    * Numbers are translated from text into actual numbers.
    * Tuples, separated in the cell by the ``::`` string, are reconstructed.
    * ``True`` and ``False`` are transformed to boolean values.
    * Fields with the value ``(Unknown)`` are dropped.

    """

    format = "Excel"
    extractor = ExcelExtractor

    def __init__(self, filepath):
        self.strategies = [
            csv_restore_tuples,
            csv_restore_booleans,
            csv_numerize,
            csv_drop_unknown,
            csv_add_missing_exchanges_section,
            normalize_units,
            normalize_biosphere_categories,
            normalize_biosphere_names,
            strip_biosphere_exc_locations,
            set_code_by_activity_hash,
            functools.partial(
                link_iterable_by_fields,
                other=Database(config.biosphere),
                kind="biosphere",
            ),
            assign_only_product_as_production,
            link_technosphere_by_activity_hash,
            drop_falsey_uncertainty_fields_but_keep_zeros,
            convert_uncertainty_types_to_integers,
            convert_activity_parameters_to_list,
        ]
        start = time()
        data = self.extractor.extract(filepath)
        data = [(x, y) for x, y in data if valid_first_cell(x, y)]
        print(
            "Extracted {} worksheets in {:.2f} seconds".format(
                len(data), time() - start
            )
        )
        if data and any(line for line in data):
            self.db_name, self.metadata = self.get_database(data)
            self.project_parameters = self.get_project_parameters(data)
            self.database_parameters = self.get_database_parameters(data)
            self.data = self.process_activities(data)
        else:
            warnings.warn("No data in workbook found")

    def get_database(self, data):
        results = []
        found = False
        for sn, ws in data:
            for index, line in enumerate(ws):
                if line and hasattr(line[0], "lower") and line[0].lower() == "database":
                    if found:
                        raise ValueError("Multiple `database` sections found")
                    results.append(self.get_metadata_section(sn, ws, index))
                    found = True

        if not results:
            raise ValueError("No `database` section found")

        return results[0]

    def get_database_parameters(self, data):
        parameters, found = [], False
        for sn, ws in data:
            for index, line in enumerate(ws):
                if (
                    line
                    and hasattr(line[0], "lower")
                    and line[0].strip().lower() == "database parameters"
                    and (len(line) == 1 or not any(line[1:]))
                ):
                    parameters.extend(self.get_labelled_section(sn, ws, index + 1))
                    found = True

        if not found:
            return None
        else:
            return parameters

    def get_project_parameters(self, data):
        """Extract project parameters (variables and formulas).

        Project parameters are a section that starts with a line with the string "project parameters" (case-insensitive) in the first cell, and ends with a blank line. There can be multiple project parameter sections."""
        parameters, found = [], False
        for sn, ws in data:
            for index, line in enumerate(ws):
                if (
                    line
                    and hasattr(line[0], "lower")
                    and line[0].strip().lower() == "project parameters"
                ):
                    parameters.extend(self.get_labelled_section(sn, ws, index + 1))
                    found = True

        if not found:
            return None
        else:
            return parameters

    def get_labelled_section(self, sn, ws, index=0, transform=True):
        """Turn a list of rows into a list of dictionaries.

        The first line of ``ws`` is the column labels. All subsequent rows are the data values. Missing columns are dropped.

        ``transform`` is a boolean: perform CSV transformation functions like ``csv_restore_tuples``."""
        data = []
        ws = ws[index:]
        columns = ws[0]

        # Columns can be ['foo', '', 'bar', ''] - find last existing value.
        # Can't just test for boolean-like behaviour, unfortunately
        for index, elem in enumerate(columns[-1:0:-1]):
            if elem:
                break
        if index:
            columns = columns[:-index]
        assert columns, "No label columns found"
        assert all(columns), "Missing column labels: {}:{}\n{}".format(
            sn, index, columns
        )

        ws = ws[1:]
        for row in ws:
            if is_empty_line(row):
                break
            data.append({x: y for x, y in zip(columns, row)})

        if transform:
            data = csv_restore_tuples(
                csv_restore_booleans(csv_numerize(csv_drop_unknown(data)))
            )

        return [remove_empty(o) for o in data]

    def get_metadata_section(self, sn, ws, index=0, transform=True):
        data = {}
        ws = ws[index:]

        name = ws[0][1]
        assert name, "Must provide valid name for metadata section (got '{}')".format(
            name
        )

        for row in ws[1:]:
            if is_empty_line(row):
                break
            data[row[0]] = row[1]

        if transform:
            # Only need first element
            data = csv_restore_tuples(
                csv_restore_booleans(csv_numerize(csv_drop_unknown([data])))
            )[0]

        return name, data

    def process_activities(self, data):
        """Take list of `(sheet names, raw data)` and process it."""
        new_activity = lambda x: (
            isinstance(x[0], str)
            and isinstance(x[1], str)
            and x[0].strip().lower() == "activity"
        )

        def cut_worksheet(obj):
            if isinstance(obj[0][0], str) and obj[0][0].lower() == "cutoff":
                try:
                    cutoff = int(obj[0][1])
                except:
                    raise ValueError("Can't understand cutoff index")
                return [x[:cutoff] for x in obj]
            else:
                return obj

        results = []

        for sn, ws in data:
            ws = cut_worksheet(ws)
            if not any(line for line in ws):
                warnings.warn("All data cutoff in worksheet {}".format(sn))
                continue
            for index, line in enumerate(ws):
                if new_activity(line):
                    results.append(self.get_activity(sn, ws[index:]))

        return results

    def write_activity_parameters(self, data=None, delete_existing=True):
        self._write_activity_parameters(
            self._prepare_activity_parameters(data, delete_existing)
        )

    def write_database_parameters(self, activate_parameters=True, delete_existing=True):
        """Same as base ``write_database_parameters`` method, but ``activate_parameters`` is True by default."""
        super(ExcelImporter, self).write_database_parameters(
            activate_parameters, delete_existing
        )

    def write_database(self, **kwargs):
        """Same as base ``write_database`` method, but ``activate_parameters`` is True by default."""
        kwargs["activate_parameters"] = kwargs.get("activate_parameters", True)
        super(ExcelImporter, self).write_database(**kwargs)

    def get_activity(self, sn, ws):
        activity_end = lambda x: (
            isinstance(x[0], str)
            and x[0].strip().lower() in ("activity", "database", "project parameters")
        )
        exc_section = lambda x: (
            isinstance(x[0], str) and x[0].strip().lower() == "exchanges"
        )
        param_section = lambda x: (
            isinstance(x[0], str) and x[0].strip().lower() == "parameters"
        )

        end = None
        found_next_section = False
        for end, row in enumerate(ws[1:]):
            if activity_end(row):
                found_next_section = True
                break

        # Started from row 1, not row 0
        end += 1
        # Worksheet ends; `end` is too small by 1
        if not found_next_section:
            end += 1

        ws = [row for row in ws[:end] if not is_empty_line(row)]

        param_index = exc_index = None
        for index, row in enumerate(ws):
            if param_section(row):
                if param_index is not None:
                    raise ValueError("Multiple parameter sections in activity")
                param_index = index
            elif exc_section(row):
                if exc_index is not None:
                    raise ValueError("Multiple exchanges sections in activity")
                exc_index = index

        if param_index is None:
            if exc_index is None:
                metadata, parameters, exchanges = ws, None, None
            else:
                metadata = ws[:exc_index]
                parameters = None
                exchanges = ws[exc_index + 1 :]
        elif exc_index is None:
            metadata = ws[:param_index]
            parameters = ws[param_index:]
            exchanges = []
        else:
            metadata = ws[:param_index]
            parameters = ws[param_index:exc_index]
            exchanges = ws[exc_index + 1 :]

        name, data = self.get_metadata_section(sn, metadata, transform=False)
        data["name"] = name

        if parameters and len(parameters) > 1:
            try:
                group_name = parameters[0][1]
                assert group_name
            except:
                group_name = None
            data["parameters"] = {
                e.pop("name"): e for e in self.get_labelled_section(sn, parameters[1:])
            }
            if group_name:
                for ds in data["parameters"].values():
                    ds["group"] = group_name

        if exchanges:
            data["exchanges"] = self.get_labelled_section(
                sn, exchanges, transform=False
            )
        else:
            data["exchanges"] = []

        data["worksheet name"] = sn
        data["database"] = self.db_name
        return data


class CSVImporter(ExcelImporter):
    """Generic CSV importer"""

    format = "CSV"
    extractor = CSVExtractor
