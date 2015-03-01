from ..compatibility import SIMAPRO_BIOSPHERE
from ..units import normalize_units
from bw2data import Database, databases
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
import codecs
import json
import os
import warnings
import xlrd


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

    Format is ``(first level category, old name, new name)``. First-level categories are already in 3.1 naming convention."""
    sheet_23 = get_sheet(os.path.join(dirpath, "ecoinvent elementary flows 2-3.xls"), "ElementaryExchanges")
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
    ws = get_sheet(os.path.join(dirpath, "SimaPro - ecoinvent - biosphere.xlsx"), "ee")
    data = [[ws.cell(row, col).value for col in range(3)]
            for row in range(1, ws.nrows)]
    data = {[SIMAPRO_BIOSPHERE[obj[0]], obj[1], obj[2]] for obj in data}
    write_json_file(sorted(data), 'simapro-biosphere')


def convert_simapro_ecoinvent_activities():
    """Write a correspondence list from SimaPro activity names to ecoinvent 3 processes to a JSON file."""
    ws = get_sheet(os.path.join(dirpath, "SimaPro - ecoinvent - technosphere.xlsx"), "Mapping")
    data = [[ws.cell(row, col).value for col in range(1, 7)]
            for row in range(3, ws.nrows)]
    write_json_file(data, 'simapro-ecoinvent31')


def create_biosphere3(backend=None):
    EMISSIONS_CATEGORIES = {
        "air":   "emission",
        "soil":  "emission",
        "water": "emission",
    }

    def extract_metadata(o):
        ds = {
            'categories': (
                o.compartment.compartment.text,
                o.compartment.subcompartment.text
            ),
            'code': o.get('id'),
            'name': o.name.text,
            'database': 'biosphere3',
            'exchanges': [],
            'unit': normalize_units(o.unitName.text),
        }
        ds[u"type"] = EMISSIONS_CATEGORIES.get(
            ds['categories'][0], ds['categories'][0]
        )
        return ds

    fp = os.path.join(dirpath, "ecoinvent elementary flows 3.1.xml")
    root = objectify.parse(open(fp)).getroot()
    data = recursive_str_to_unicode([extract_metadata(ds)
                                     for ds in root.iterchildren()])

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        db = Database(u'biosphere3', backend=backend)
    if 'biosphere3' not in databases:
        db.register()
    db.write({(ds['database'], ds['code']): ds for ds in data})
    db.process()
    return db
