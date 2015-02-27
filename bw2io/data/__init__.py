from ..compatibility import SIMAPRO_BIOSPHERE
from ..units import normalize_units
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


def convert_simapro_ecoinvent_elementary_flows():
    """Return a correspondence list from SimaPro elementary flow names to ecoinvent 3 flow names."""
    ws = get_sheet(os.path.join(dirpath, "SimaPro - ecoinvent - biosphere.xlsx"), "ee")
    data = [[ws.cell(row, col).value for col in range(3)]
            for row in range(1, ws.nrows)]
    data = [[SIMAPRO_BIOSPHERE[obj[0]], obj[1], obj[2]] for obj in data]
    write_json_file(data, 'simapro-biosphere')


def convert_simapro_ecoinvent_activity_correspondence():
    ws = get_sheet(os.path.join(dirpath, "SimaPro - ecoinvent.xlsx"), "ee")
    data = [[ws.cell(row, col).value for col in range(3)]
            for row in range(1, ws.nrows)]
    data = [[SIMAPRO_BIOSPHERE[obj[0]], obj[1], obj[2]] for obj in data]
    write_json_file(data, 'simapro-biosphere')


def create_biosphere3(self, backend=None):
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
