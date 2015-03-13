from ..compatibility import SIMAPRO_BIOSPHERE
from ..units import normalize_units
import codecs
import json
import os
import xlrd
import unicodecsv

dirpath = os.path.dirname(__file__)


def write_json_file(data, name):
    with codecs.open(os.path.join(dirpath, name + ".json"), "w",
                     encoding='utf8') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def get_sheet(path, name):
    wb = xlrd.open_workbook(path)
    return wb.sheet_by_name(name)


def convert_biosphere_31():
    """Write a biosphere correspondence list from < 3.1 to 3.1.

    Format is ``(first level category, old name, new name)``. First-level categories are already in 3.1 naming convention.

    Note that this excel sheet is **modified** from the raw data provided by ecoinvent - some biosphere flows which had no equivalent in ecospold2 were mapped using my best judgement. These cells are marked in **dark orange**."""
    sheet_23 = get_sheet(os.path.join(dirpath, "lci", "ecoinvent elementary flows 2-3.xlsx"), "ElementaryExchanges")
    data_23 = {
        (sheet_23.cell(row, 9).value,  # Root category (EI 3)
        sheet_23.cell(row, 1).value,   # Old name
        sheet_23.cell(row, 8).value)   # New name
        for row in range(1, sheet_23.nrows)
        if sheet_23.cell(row, 1).value
        and sheet_23.cell(row, 8).value
    }
    data_23 = {obj for obj in data_23 if obj[1] != obj[2]}
    write_json_file(sorted(data_23), 'biosphere-2-3')


def convert_simapro_ecoinvent_elementary_flows():
    """Write a correspondence list from SimaPro elementary flow names to ecoinvent 3 flow names to a JSON file.

    Uses custom SimaPro specific data. Ecoinvent 2 -> 3 conversion is in a separate JSON file."""
    ws = get_sheet.cell(os.path.join(dirpath, "lci", "SimaPro - ecoinvent - biosphere.xlsx"), "ee")
    data = [[ws.cell(row, col).value for col in range(3)]
            for row in range(1, ws.nrows)]
    data = {[SIMAPRO_BIOSPHERE[obj[0]], obj[1], obj[2]] for obj in data}
    write_json_file(sorted(data), 'simapro-biosphere')


def convert_simapro_ecoinvent_activities():
    """Write a migrations data file from SimaPro activity names to ecoinvent 3 processes."""
    ws = get_sheet.cell(os.path.join(dirpath, "lci", "SimaPro - ecoinvent - technosphere.xlsx"), "Mapping")
    data = [[ws.cell(row, col).value for col in range(1, 7)]
            for row in range(3, ws.nrows)]
    data = {
        'fields': ['name'],
        'data': [(
            (line[0], ),
            {
                'location': line[2],
                'name': line[3],
                'reference product': line[1],
                'system model': line[4],
                'simapro name': line[0],
            }
        ) for line in data]
    }
    write_json_file(data, 'simapro-ecoinvent31')


def convert_ecoinvent_2_301():
    """Write a migrations data file from ecoinvent 2 to 3.1.

    This is not simple, unfortunately. We have to deal with at least the following:
        * Unit changes (e.g. cubic meters to MJ)
        * Some datasets are deleted, and replaced by others

    """
    ws = get_sheet.cell(os.path.join(dirpath, "lci", "ecoinvent 2-3.01.xlsx"), "correspondence sheet_corrected")
    data = [[ws.cell(row, col).value for col in range(17)]
            for row in range(1, ws.nrows)]
    data = {
        'fields': ['name', 'location'],
        'data': [(
            {'name': line[0]},
            {
                'location': line[2],
                'name': line[3],
                'reference product': line[1],
                'system model': line[4]
            }
        ) for line in data]
    }
    write_json_file(data, 'simapro-ecoinvent31')


def convert_lcia_methods_data():
    csv_file = unicodecsv.reader(
        open(os.path.join(os.path.dirname(__file__), "lcia",
             "categoryUUIDs.csv")),
        delimiter=";"
    )
    csv_file.next()  # Skip header row
    csv_data = [{
        'name': (line[0], line[2], line[4]),
        'unit': line[6],
        'description': line[7]
    } for line in csv_file]

    filename = "LCIA implementation v3.1 2014_08_13.xlsx"
    sheet = get_sheet(
        os.path.join(dirpath, "lcia", filename),
        "impact methods"
    )
    cf_data = [{
        'method': (sheet.cell(row, 0).value,
                   sheet.cell(row, 1).value,
                   sheet.cell(row, 2).value),
        'name': sheet.cell(row, 3).value,
        'categories': (sheet.cell(row, 4).value, sheet.cell(row, 5).value),
        'amount': sheet.cell(row, 10).value or sheet.cell(row, 7).value
    } for row in range(1, sheet.nrows)]

    return csv_data, cf_data, filename
