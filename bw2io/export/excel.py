# _*_ coding: utf-8
from bw2calc import LCA
from bw2data import config, Database
from bw2data.utils import safe_filename
import os
import scipy.io
import xlsxwriter


def lci_matrices_to_excel(database_name, include_descendants=True):
    safe_name = safe_filename(database_name, False)
    dirpath = config.request_dir(u"export")
    filepath = os.path.join(dirpath, safe_name + ".xlsx")

    lca = LCA({Database(database_name).random(): 1})
    lca.load_lci_data()
    lca.fix_dictionaries()

    if not include_descendants:
        lca.technosphere_dict = {
            key: value
            for key, value in lca.technosphere_dict.items()
            if key[0] == database_name
        }

    # Drop biosphere flows with zero references
    lca.biosphere_dict = {
        key: value
        for key, value in lca.biosphere_dict.items()
        if lca.biosphere_matrix[lca.biosphere_dict[key], :].sum() != 0
    }

    rt, rb = lca.reverse_dict()

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({'bold': True})

    biosphere_dicts = {}
    technosphere_dicts = {}
    sorted_tech_keys = [
        x[1] for x in
        sorted([
            (Database(key[0]).load()[key].get("name", u"Unknown"), key)
            for key in lca.technosphere_dict
        ])
    ]
    sorted_bio_keys = sorted(lca.biosphere_dict.keys())


    tm_sheet = workbook.add_worksheet('technosphere')
    tm_sheet.set_column('A:A', 50)

    data = Database(database_name).load()

    for index, key in enumerate(sorted_tech_keys):
        if key[0] not in technosphere_dicts:
            technosphere_dicts[key[0]] = Database(key[0]).load()
        obj = technosphere_dicts[key[0]][key]

        tm_sheet.write_string(index + 1, 0, obj.get(u'name', u'Unknown'))
        tm_sheet.write_string(0, index + 1, obj.get(u'name', u'Unknown'))

    for row, row_key in enumerate(sorted_tech_keys):
        row_i = lca.technosphere_dict[row_key]
        for col, col_key in enumerate(sorted_tech_keys):
            col_i = lca.technosphere_dict[col_key]
            if lca.technosphere_matrix[row_i, col_i] != 0:
                tm_sheet.write_number(
                    row + 1,
                    col + 1,
                    float(lca.technosphere_matrix[row_i, col_i])
                )

    bm_sheet = workbook.add_worksheet('biosphere')
    bm_sheet.set_column('A:A', 50)

    data = Database(database_name).load()

    for index, key in enumerate(sorted_tech_keys):
        obj = technosphere_dicts[key[0]][key]
        bm_sheet.write_string(0, index + 1, obj.get(u'name', u'Unknown'))

    for index, key in enumerate(sorted_bio_keys):
        if key[0] not in biosphere_dicts:
            biosphere_dicts[key[0]] = Database(key[0]).load()
        obj = biosphere_dicts[key[0]][key]
        bm_sheet.write_string(index + 1, 0, obj.get(u'name', u'Unknown'))

    for row, row_key in enumerate(sorted_bio_keys):
        row_i = lca.biosphere_dict[row_key]
        for col, col_key in enumerate(sorted_tech_keys):
            col_i = lca.technosphere_dict[col_key]
            if lca.technosphere_matrix[row_i, col_i] != 0:
                tm_sheet.write_number(
                    row + 1,
                    col + 1,
                    float(lca.biosphere_matrix[row_i, col_i])
                )

    COLUMNS = (
        u"Index",
        u"Name",
        u"Reference product",
        u"Unit",
        u"Categories",
        u"Location"
    )

    tech_sheet = workbook.add_worksheet('technosphere-labels')
    tech_sheet.set_column('B:B', 60)
    tech_sheet.set_column('C:C', 30)
    tech_sheet.set_column('D:D', 15)
    tech_sheet.set_column('E:E', 30)

    # Header
    for index, col in enumerate(COLUMNS):
        tech_sheet.write_string(0, index, col, bold)

    tech_sheet.write_comment(
        'C1',
        "Only for ecoinvent 3, where names =/= products.",
    )

    for index, key in enumerate(sorted_tech_keys):
        if key[0] not in technosphere_dicts:
            technosphere_dicts[key[0]] = Database(key[0]).load()
        obj = technosphere_dicts[key[0]][key]

        tech_sheet.write_number(index + 1, 0, index + 1)
        tech_sheet.write_string(index + 1, 1, obj.get(u'name', u'Unknown'))
        tech_sheet.write_string(index + 1, 2, obj.get(u'reference product', u''))
        tech_sheet.write_string(index + 1, 3, obj.get(u'unit', u'Unknown'))
        tech_sheet.write_string(index + 1, 4, u" - ".join(obj.get(u'categories', [])))
        tech_sheet.write_string(index + 1, 5, obj.get(u'location', u'Unknown'))

    COLUMNS = (
        u"Index",
        u"Name",
        u"Unit",
        u"Categories",
    )

    bio_sheet = workbook.add_worksheet('biosphere-labels')
    bio_sheet.set_column('B:B', 60)
    bio_sheet.set_column('C:C', 15)
    bio_sheet.set_column('D:D', 30)

    # Header
    for index, col in enumerate(COLUMNS):
        bio_sheet.write_string(0, index, col, bold)

    for index, key in enumerate(sorted_bio_keys):
        if key[0] not in biosphere_dicts:
            biosphere_dicts[key[0]] = Database(key[0]).load()
        obj = biosphere_dicts[key[0]][key]

        bio_sheet.write_number(index + 1, 0, index + 1)
        bio_sheet.write_string(index + 1, 1, obj.get(u'name', u'Unknown'))
        bio_sheet.write_string(index + 1, 2, obj.get(u'unit', u'Unknown'))
        bio_sheet.write_string(index + 1, 3, u" - ".join(obj.get(u'categories', [])))

    workbook.close()
    return filepath


def write_lci_matching(db, database_name):
    """Write matched and unmatched exchanges to Excel file"""
    def write_headers(sheet, row):
        columns = (
            'Name',
            'Unit',
            'Categories',
            'Location',
            'Type',
            'Matched'
        )
        for index, col in enumerate(columns):
            sheet.write_string(row, index, col, bold)

    def write_row(sheet, row, data, exc=True):
        if exc:
            sheet.write_string(row, 0, data.get('name', '(unknown)'))
        else:
            sheet.write_string(row, 0, data.get('name', '(unknown)'), bold)
        sheet.write_string(row, 1, data.get('unit', '(unknown)'))
        sheet.write_string(row, 2, u":".join(data.get('categories', ['(unknown)'])))
        sheet.write_string(row, 3, data.get('location', '(unknown)'))
        if exc:
            sheet.write_string(row, 4, data.get('type', '(unknown)'))
            sheet.write_boolean(row, 5, 'input' in data)

    safe_name = safe_filename(database_name, False)
    dirpath = config.request_dir(u"export")
    filepath = os.path.join(dirpath, u"db-matching-" + safe_name + u".xlsx")

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({'bold': True})
    bold.set_font_size(12)
    sheet = workbook.add_worksheet('matching')
    sheet.set_column('A:A', 60)
    sheet.set_column('B:B', 12)
    sheet.set_column('C:C', 40)
    sheet.set_column('D:D', 12)
    sheet.set_column('E:E', 12)

    row = 0
    for ds in db:
        write_row(sheet, row, ds, False)
        write_headers(sheet, row + 1)
        row += 2
        for exc in sorted(ds.get('exchanges', []),
                          key=lambda x: x.get('name')):
            write_row(sheet, row, exc)
            row += 1
        row += 1

    return filepath


def write_lcia_matching(db, name):
    """Write matched an unmatched CFs to Excel file"""
    def write_headers(sheet, row):
        columns = (
            'Name',
            'Unit',
            'Categories',
            'Matched'
        )
        for index, col in enumerate(columns):
            sheet.write_string(row, index, col, bold)

    def write_row(sheet, row, data):
        sheet.write_string(row, 0, data.get('name', '(unknown)'))
        sheet.write_string(row, 1, data.get('unit', '(unknown)'))
        sheet.write_string(row, 2, u":".join(data.get('categories', ['(unknown)'])))
        sheet.write_boolean(row, 3, 'code' in data)

    safe_name = safe_filename(name, False)
    dirpath = config.request_dir(u"export")
    filepath = os.path.join(dirpath, u"lcia-matching-" + safe_name + u".xlsx")

    workbook = xlsxwriter.Workbook(filepath)
    bold = workbook.add_format({'bold': True})
    bold.set_font_size(12)
    sheet = workbook.add_worksheet('matching')
    sheet.set_column('A:A', 60)
    sheet.set_column('B:B', 12)
    sheet.set_column('C:C', 40)

    row = 0
    for ds in db:
        for index, elem in enumerate(ds['name']):
            sheet.write_string(row, index, elem, bold)
        write_headers(sheet, row + 1)
        row += 2
        for cf in sorted(ds.get('data', []),
                          key=lambda x: x.get('name')):
            write_row(sheet, row, cf)
            row += 1
        row += 1

    return filepath
