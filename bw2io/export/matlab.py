# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import config, Database, projects
from bw2data.utils import safe_filename
import os
import scipy.io
import xlsxwriter


def lci_matrices_to_matlab(database_name):
    from bw2calc import LCA

    safe_name = safe_filename(database_name, False)
    projects.request_directory(u"export")
    dirpath = projects.request_directory(u"export/%s-matlab" % safe_name)

    lca = LCA({Database(database_name).random(): 1})
    lca.lci()
    lca.fix_dictionaries()
    ra, rp, rb = lca.reverse_dict()

    scipy.io.savemat(
        os.path.join(dirpath, safe_name + ".mat"),
        {
            'technosphere': lca.technosphere_matrix,
            'biosphere': lca.biosphere_matrix
        }
    )

    workbook = xlsxwriter.Workbook(os.path.join(dirpath, safe_name + ".xlsx"))
    bold = workbook.add_format({'bold': True})

    COLUMNS = (
        u"Index",
        u"Name",
        u"Reference product",
        u"Unit",
        u"Categories",
        u"Location"
    )

    tech_sheet = workbook.add_worksheet('technosphere')
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

    data = Database(database_name).load()

    for index, key in sorted(ra.items()):
        tech_sheet.write_number(index + 1, 0, index + 1)
        tech_sheet.write_string(index + 1, 1, data[key].get(u'name') or u'Unknown')
        tech_sheet.write_string(index + 1, 2, data[key].get(u'reference product') or u'')
        tech_sheet.write_string(index + 1, 3, data[key].get(u'unit') or u'Unknown')
        tech_sheet.write_string(index + 1, 4, u" - ".join(data[key].get(u'categories') or []))
        tech_sheet.write_string(index + 1, 5, data[key].get(u'location') or u'Unknown')

    COLUMNS = (
        u"Index",
        u"Name",
        u"Unit",
        u"Categories",
    )

    biosphere_dicts = {}
    bio_sheet = workbook.add_worksheet('biosphere')
    bio_sheet.set_column('B:B', 60)
    bio_sheet.set_column('C:C', 15)
    bio_sheet.set_column('D:D', 30)

    # Header
    for index, col in enumerate(COLUMNS):
        bio_sheet.write_string(0, index, col, bold)

    for index, key in sorted(rb.items()):
        if key[0] not in biosphere_dicts:
            biosphere_dicts[key[0]] = Database(key[0]).load()
        obj = biosphere_dicts[key[0]][key]

        bio_sheet.write_number(index + 1, 0, index + 1)
        bio_sheet.write_string(index + 1, 1, obj.get(u'name', u'Unknown'))
        bio_sheet.write_string(index + 1, 2, obj.get(u'unit', u'Unknown'))
        bio_sheet.write_string(index + 1, 3, u" - ".join(obj.get(u'categories', [])))

    workbook.close()
    return dirpath
