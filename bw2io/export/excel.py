import collections
import numbers
import os

import xlsxwriter
from bw2data import Database, projects
from bw_processing import safe_filename

from ..utils import activity_hash
from .csv import CSVFormatter


def create_valid_worksheet_name(string):
    # """Exclude invalid characters and names.

    # Data from http://www.accountingweb.com/technology/excel/seven-characters-you-cant-use-in-worksheet-names."""
    """
    Exclude invalid characters and names.

    Parameters
    ----------
    string : str
        String to convert to a valid worksheet name.

    Returns
    -------
    string : str
        Valid worksheet name.

    Notes
    -----
    Data from http://www.accountingweb.com/technology/excel/seven-characters-you-cant-use-in-worksheet-names.
    """

    excluded = {"\\", "/", "*", "[", "]", ":", "?"}

    if string == "History":
        return "History-worksheet"
    for x in excluded:
        string = string.replace(x, "#")
    return string[:30]


def lci_matrices_to_excel(database_name, include_descendants=True):

    # """Fake docstring"""
    """
    Export LCI matrices to Excel.

    Parameters
    ----------
    database_name : str
        Name of database to export.
    include_descendants : bool
        Include databases which are linked from ``database_name``. (default True)

    Returns
    -------
    filepath : str
        Path to created Excel file.

    Warning
    -------
    ``include_descendants`` is not yet implemented.

    Notes
    -----
    There are a TODO in the code.

    Examples
    --------
    >>> lci_matrices_to_excel(database_name='example_db', include_descendants=True)
    '/path/to/example_db.xlsx'
    """

    from bw2calc import LCA

    print("Starting Excel export. This can be slow for large matrices!")
    safe_name = safe_filename(database_name, False)
    filepath = os.path.join(projects.output_dir, safe_name + ".xlsx")

    lca = LCA({Database(database_name).random(): 1})
    lca.load_lci_data()
    lca.fix_dictionaries()

    if not include_descendants:
        lca.activity_dict = {
            key: value
            for key, value in lca.activity_dict.items()
            if key[0] == database_name
        }

    # Drop biosphere flows with zero references
    # TODO: This will ignore (-1 + 1 = 0) references
    lca.biosphere_dict = {
        key: value
        for key, value in lca.biosphere_dict.items()
        if lca.biosphere_matrix[lca.biosphere_dict[key], :].sum() != 0
    }

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({"bold": True})

    print("Sorting objects")

    sorted_activity_keys = sorted(
        [
            (Database.get(key).get("name") or u"Unknown", key)
            for key in lca.activity_dict
        ]
    )
    sorted_product_keys = sorted(
        [(Database.get(key).get("name") or u"Unknown", key) for key in lca.product_dict]
    )
    sorted_bio_keys = sorted(
        [
            (Database.get(key).get("name") or u"Unknown", key)
            for key in lca.biosphere_dict
        ]
    )

    tm_sheet = workbook.add_worksheet("technosphere")
    tm_sheet.set_column("A:A", 50)

    data = Database(database_name).load()

    # Labels
    for index, data in enumerate(sorted_activity_keys):
        tm_sheet.write_string(0, index + 1, data[0])
    for index, data in enumerate(sorted_product_keys):
        tm_sheet.write_string(index + 1, 0, data[0])

    print("Entering technosphere matrix data")

    coo = lca.technosphere_matrix.tocoo()

    # Translate row index to sorted product index
    act_dict = {obj[1]: idx for idx, obj in enumerate(sorted_activity_keys)}
    pro_dict = {obj[1]: idx for idx, obj in enumerate(sorted_product_keys)}
    bio_dict = {obj[1]: idx for idx, obj in enumerate(sorted_bio_keys)}

    pro_lookup = {v: pro_dict[k] for k, v in lca.product_dict.items()}
    bio_lookup = {v: bio_dict[k] for k, v in lca.biosphere_dict.items()}
    act_lookup = {v: act_dict[k] for k, v in lca.activity_dict.items()}

    # Matrix values
    for row, col, value in zip(coo.row, coo.col, coo.data):
        tm_sheet.write_number(pro_lookup[row] + 1, act_lookup[col] + 1, value)

    bm_sheet = workbook.add_worksheet("biosphere")
    bm_sheet.set_column("A:A", 50)

    data = Database(database_name).load()

    # Labels
    for index, data in enumerate(sorted_activity_keys):
        bm_sheet.write_string(0, index + 1, data[0])
    for index, data in enumerate(sorted_bio_keys):
        bm_sheet.write_string(index + 1, 0, data[0])

    print("Entering biosphere matrix data")

    coo = lca.biosphere_matrix.tocoo()

    # Matrix values
    for row, col, value in zip(coo.row, coo.col, coo.data):
        bm_sheet.write_number(bio_lookup[row] + 1, act_lookup[col] + 1, value)

    COLUMNS = (
        u"Index",
        u"Name",
        u"Reference product",
        u"Unit",
        u"Categories",
        u"Location",
    )

    tech_sheet = workbook.add_worksheet("technosphere-labels")
    tech_sheet.set_column("B:B", 60)
    tech_sheet.set_column("C:C", 30)
    tech_sheet.set_column("D:D", 15)
    tech_sheet.set_column("E:E", 30)

    print("Writing metadata")

    # Header
    for index, col in enumerate(COLUMNS):
        tech_sheet.write_string(0, index, col, bold)

    tech_sheet.write_comment(
        "C1",
        "Only for ecoinvent 3, where names =/= products.",
    )

    for index, data in enumerate(sorted_activity_keys):
        obj = Database.get(data[1])

        tech_sheet.write_number(index + 1, 0, index + 1)
        tech_sheet.write_string(index + 1, 1, obj.get(u"name") or u"Unknown")
        tech_sheet.write_string(index + 1, 2, obj.get(u"reference product") or u"")
        tech_sheet.write_string(index + 1, 3, obj.get(u"unit") or u"Unknown")
        tech_sheet.write_string(index + 1, 4, u" - ".join(obj.get(u"categories") or []))
        tech_sheet.write_string(index + 1, 5, obj.get(u"location") or u"Unknown")

    COLUMNS = (
        u"Index",
        u"Name",
        u"Unit",
        u"Categories",
    )

    bio_sheet = workbook.add_worksheet("biosphere-labels")
    bio_sheet.set_column("B:B", 60)
    bio_sheet.set_column("C:C", 15)
    bio_sheet.set_column("D:D", 30)

    # Header
    for index, col in enumerate(COLUMNS):
        bio_sheet.write_string(0, index, col, bold)

    for index, data in enumerate(sorted_bio_keys):
        obj = Database.get(data[1])

        bio_sheet.write_number(index + 1, 0, index + 1)
        bio_sheet.write_string(index + 1, 1, obj.get(u"name") or u"Unknown")
        bio_sheet.write_string(index + 1, 2, obj.get(u"unit") or u"Unknown")
        bio_sheet.write_string(index + 1, 3, u" - ".join(obj.get(u"categories") or []))

    workbook.close()
    return filepath


def write_lci_excel(database_name, objs=None, sections=None, dirpath=None):
    # """Export database `database_name` to an Excel spreadsheet.

    # Not all data can be exported. The following constraints apply:

    # * Nested data, e.g. `{'foo': {'bar': 'baz'}}` are excluded. Spreadsheets are not a great format for nested data. However, *tuples* are exported, and the characters `::` are used to join elements of the tuple.
    # * The only well-supported data types are strings, numbers, and booleans.

    # Default directory is ``projects.output_dir``, set ``dirpath`` to have save the file somewhere else.

    # Returns the filepath of the exported file.

    # """
    """
    Export database `database_name` to an Excel spreadsheet.

    Notes
    -----
    Not all data can be exported. The following constraints apply:

    * Nested data, e.g. `{'foo': {'bar': 'baz'}}` are excluded. Spreadsheets are not a great format for nested data. However, *tuples* are exported, and the characters `::` are used to join elements of the tuple.
    * The only well-supported data types are strings, numbers, and booleans.

    Default directory is ``projects.output_dir``, set ``dirpath`` to have save the file somewhere else.

    Parameters
    ----------
    database_name : str
        Name of the database to export.
    objs : list, optional
        List of objects to export. If not provided, all objects in the database will be exported.
    sections : list, optional
        List of sections to export. If not provided, all sections will be exported.
    dirpath : str, optional
        Directory to save the file to. Default is ``projects.output_dir``.

    Returns
    -------
    str
        Filepath of the exported file.
    """
    
    safe_name = safe_filename(database_name, False)
    if dirpath is None:
        dirpath = projects.output_dir
    if not os.path.isdir(dirpath) or not os.access(dirpath, os.W_OK):
        raise ValueError(f"Directory path {dirpath} is not a writable directory")
    filepath = os.path.join(dirpath, "lci-" + safe_name + ".xlsx")

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({"bold": True})
    bold.set_font_size(12)
    highlighted = {
        "Activity",
        "Database",
        "Exchanges",
        "Parameters",
        "Database parameters",
        "Project parameters",
    }
    frmt = lambda x: bold if row[0] in highlighted else None

    sheet = workbook.add_worksheet(create_valid_worksheet_name(database_name))

    data = CSVFormatter(database_name, objs).get_formatted_data(sections)

    for row_index, row in enumerate(data):
        for col_index, value in enumerate(row):
            if value is None:
                continue
            elif isinstance(value, numbers.Number):
                sheet.write_number(row_index, col_index, value, frmt(value))
            else:
                try:
                    sheet.write_string(row_index, col_index, value, frmt(value))
                except TypeError:
                    pass

    workbook.close()

    return filepath


def write_lci_matching(
    db, database_name, only_unlinked=False, only_activity_names=False
):
    # """Write matched and unmatched exchanges to Excel file"""
    """
    Write matched and unmatched exchanges to Excel file

    Parameters
    ----------
    db : :class:`bw2data.Database`
        Database to write.
    database_name : str
        Name of the database to write.
    only_unlinked : bool, optional
        Only write unlinked exchanges. Default is ``False``.
    only_activity_names : bool, optional
        Only write activity names. Default is ``False``.

    Returns
    -------
    str
        Filepath of the exported file.
    """

    def write_headers(sheet, row):
        columns = (
            "Name",
            "Reference Product",
            "Amount",
            "Database",
            "Unit",
            "Categories",
            "Location",
            "Type",
            "Matched",
        )
        for index, col in enumerate(columns):
            sheet.write_string(row, index, col, bold)

    def write_row(sheet, row, data, exc=True):
        style = highlighted if ("input" not in data and exc) else None
        if exc:
            sheet.write_string(row, 0, data.get("name", "(unknown)"), style)
            sheet.write_string(
                row, 1, data.get("reference product", "(unknown)"), style
            )
            try:
                sheet.write_number(row, 2, float(data.get("amount")), style)
            except ValueError:
                sheet.write_string(row, 2, "Unknown", style)
        else:
            sheet.write_string(row, 0, data.get("name", "(unknown)"), bold)
        sheet.write_string(row, 3, data.get("input", [""])[0], style)
        sheet.write_string(row, 4, data.get("unit", "(unknown)"), style)
        sheet.write_string(
            row, 5, u":".join(data.get("categories", ["(unknown)"])), style
        )
        sheet.write_string(row, 6, data.get("location", "(unknown)"), style)
        if exc:
            sheet.write_string(row, 7, data.get("type", "(unknown)"), style)
            sheet.write_boolean(row, 8, "input" in data, style)

    if only_unlinked and only_activity_names:
        raise ValueError(
            "Must choose only one of ``only_unlinked`` and ``only_activity_names``"
        )

    safe_name = safe_filename(database_name, False)
    suffix = "-unlinked" if only_unlinked else "-names" if only_activity_names else ""
    filepath = os.path.join(
        projects.output_dir, "db-matching-" + safe_name + suffix + ".xlsx"
    )

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({"bold": True})
    highlighted = workbook.add_format({"bg_color": "#FFB5B5"})
    bold.set_font_size(12)
    sheet = workbook.add_worksheet("matching")
    sheet.set_column("A:A", 60)
    sheet.set_column("B:B", 12)
    sheet.set_column("C:C", 12)
    sheet.set_column("D:D", 20)
    sheet.set_column("E:E", 40)
    sheet.set_column("F:F", 12)
    sheet.set_column("G:G", 12)

    row = 0

    if only_unlinked:
        unique_unlinked = collections.defaultdict(set)
        hash_dict = {}
        for ds in db:
            for exc in (e for e in ds.get("exchanges", []) if not e.get("input")):
                ah = activity_hash(exc)
                unique_unlinked[exc.get("type")].add(ah)
                hash_dict[ah] = exc

        for key in sorted(unique_unlinked.keys()):
            sheet.write_string(row, 0, key, bold)
            write_headers(sheet, row + 1)
            row += 2

            exchanges = [hash_dict[ah] for ah in unique_unlinked[key]]
            exchanges.sort(key=lambda x: (x["name"], list(x.get("categories", []))))
            for exc in exchanges:
                write_row(sheet, row, exc)
                row += 1

            row += 1

    else:
        for ds in db:
            if not ds.get("exchanges"):
                continue
            write_row(sheet, row, ds, False)
            if only_activity_names:
                row += 1
                continue
            write_headers(sheet, row + 1)
            row += 2
            for exc in sorted(ds.get("exchanges", []), key=lambda x: x.get("name")):
                write_row(sheet, row, exc)
                row += 1
            row += 1

    workbook.close()

    return filepath


def write_lcia_matching(db, name):
    # """Write matched and unmatched CFs to Excel file"""
    """
    Write matched and unmatched CFs to Excel file

    Parameters
    ----------
    db : :class:`bw2data.Database`
        Database to write.
    name : str
        Name of the database to write.

    Returns
    -------
    str
        Filepath of the exported file.
    """


    def write_headers(sheet, row):
        columns = ("Name", "Amount", "Unit", "Categories", "Matched")
        for index, col in enumerate(columns):
            sheet.write_string(row, index, col, bold)

    def write_row(sheet, row, data):
        sheet.write_string(row, 0, data.get("name", "(unknown)"))
        sheet.write_number(row, 1, data.get("amount", -1))
        sheet.write_string(row, 2, data.get("unit", "(unknown)"))
        sheet.write_string(row, 3, u":".join(data.get("categories", ["(unknown)"])))
        sheet.write_boolean(row, 4, "input" in data)

    safe_name = safe_filename(name, False)
    filepath = os.path.join(projects.output_dir, "lcia-matching-" + safe_name + ".xlsx")

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({"bold": True})
    bold.set_font_size(12)
    sheet = workbook.add_worksheet("matching")
    sheet.set_column("A:A", 60)
    sheet.set_column("B:B", 12)
    sheet.set_column("C:C", 12)
    sheet.set_column("D:D", 40)

    row = 0
    for ds in db:
        for index, elem in enumerate(ds["name"]):
            sheet.write_string(row, index, elem, bold)
        write_headers(sheet, row + 1)
        row += 2
        for cf in sorted(ds.get("exchanges", []), key=lambda x: x.get("name")):
            write_row(sheet, row, cf)
            row += 1
        row += 1

    workbook.close()

    return filepath
