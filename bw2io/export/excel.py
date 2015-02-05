# _*_ coding: utf-8
from .. import config, Database
from bw2calc import LCA
from bw2data.utils import safe_filename
import scipy.io
import os
try:
    import xlsxwriter
except ImportError:
    xlsxwriter = None


def lci_matrices_to_excel(database_name, include_descendants=True):
    if not xlsxwriter:
        raise ImportError(u"Excel export requires `xlsxwriter` (install with pip).")
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
