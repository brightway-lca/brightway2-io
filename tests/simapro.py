# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

# from .fixtures.simapro_reference import background as background_data
from bw2data import Database, databases, config
from bw2data.tests import BW2DataTest, bw2test
from bw2io.importers import SimaProCSVImporter
from bw2io.importers.simapro_lcia_csv import SimaProLCIACSVImporter
from bw2io.migrations import Migration, get_default_units_migration_data, get_biosphere_2_3_category_migration_data, get_biosphere_2_3_name_migration_data
# from bw2data.utils import recursive_str_to_unicode as _
# from stats_arrays import UndefinedUncertainty, NoUncertainty
from numbers import Number
import os
import sys

SP_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "simapro")


@bw2test
def test_sp_import_allocation():
    Migration("default-units").write(
        get_default_units_migration_data(),
        "Convert to default units"
    )

    sp = SimaProCSVImporter(os.path.join(SP_FIXTURES_DIR, "allocation.csv"), normalize_biosphere=False)
    sp.apply_strategies()
    assert sp.statistics() == (3, 5, 0)
    sp.write_database()

@bw2test
def test_sp_wrong_field_ordering():
    sp = SimaProCSVImporter(os.path.join(SP_FIXTURES_DIR, "new-order.csv"))
    assert len(sp.data)
    for exc in sp.data[0]['exchanges']:
        assert isinstance(exc['amount'], Number)

@bw2test
def test_damage_category_import():

    # Write the 2 item biosphere database
    database = Database("biosphere3", backend="singlefile")
    database.register()
    database.write({
        ('biosphere3', '00e73fdb-98df-4a03-8290-79931cddfd12'):
            {'categories': ('air',),
             'code': '00e73fdb-98df-4a03-8290-79931cddfd12',
             'database': 'biosphere3',
             'exchanges': [],
             'name': 'Lead-210',
             'type': 'emission',
             'unit': 'kilo Becquerel'},
        ('biosphere3', '2cfc5ba4-3db2-4193-9e81-b61e75ba1706'):
            {'categories': ('water',),
             'code': '2cfc5ba4-3db2-4193-9e81-b61e75ba1706',
             'database': 'biosphere3',
             'exchanges': [],
             'name': 'Lead-210',
             'type': 'emission',
             'unit': 'kilo Becquerel'}
    })

    assert database

    #create the required migrations
    Migration("biosphere-2-3-categories").write(
        get_biosphere_2_3_category_migration_data(),
        "Change biosphere category and subcategory labels to ecoinvent version 3"
    )
    Migration("biosphere-2-3-names").write(
        get_biosphere_2_3_name_migration_data(),
        "Change biosphere flow names to ecoinvent version 3"
    )

    # Run the import
    if sys.version_info[0] < 3:
        delimiter = b"\t"
    else:
        delimiter = "\t"
    sp = SimaProLCIACSVImporter(os.path.join(SP_FIXTURES_DIR, "damagecategory.txt"), delimiter=delimiter)

    assert len(sp.data)

    sp.apply_strategies()

    assert sp.statistics() == (6, 12, 0)


class SimaProCSVImporterTest(BW2DataTest):
    # def extra_setup(self):
    #     # SimaPro importer always wants biosphere database
    #     database = Database("biosphere", backend="singlefile")
    #     database.register()
    #     database.write({})

    # def filepath(self, name):
    #     return os.path.join(SP_FIXTURES_DIR, name + '.txt')

    # def test_invalid_file(self):
    #     sp = SimaProImporter(self.filepath("invalid"), depends=[])
    #     data = sp.load_file()
    #     with self.assertRaises(AssertionError):
    #         sp.verify_simapro_file(data)

    # def test_overwrite(self):
    #     database = Database("W00t")
    #     database.register()
    #     sp = SimaProImporter(self.filepath("empty"), depends=[], overwrite=True)
    #     sp.importer()
    #     self.assertTrue("W00t" in databases)

    # def test_no_overwrite(self):
    #     database = Database("W00t")
    #     database.register()
    #     sp = SimaProImporter(self.filepath("empty"), depends=[])
    #     with self.assertRaises(AssertionError):
    #         sp.importer()

    # def test_import_one_empty_process(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[])
    #     sp.importer()
    #     self.assertTrue("W00t" in databases)
    #     self.assertEqual(len(Database("W00t").load()), 1)

    # def test_get_db_name(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[])
    #     sp.importer()
    #     self.assertTrue("W00t" in databases)

    # def test_set_db_name(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[], name="A different one")
    #     sp.importer()
    #     self.assertTrue("A different one" in databases)
    #     self.assertTrue("W00t" not in databases)

    # def test_default_geo(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[], default_geo="Where?")
    #     sp.importer()
    #     data = Database("W00t").load().values()[0]
    #     self.assertEqual("Where?", data['location'])

    # def test_no_multioutput(self):
    #     sp = SimaProImporter(self.filepath("multioutput"), depends=[])
    #     with self.assertRaises(AssertionError):
    #         sp.importer()

    pass

    # def test_simapro_unit_conversion(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[])
    #     sp.importer()
    #     data = Database("W00t").load().values()[0]
    #     self.assertEqual("unit", data['unit'])

    # def test_dataset_definition(self):
    #     self.maxDiff = None
    #     sp = SimaProImporter(self.filepath("empty"), depends=[])
    #     sp.importer()
    #     data = Database("W00t").load().values()[0]
    #     self.assertEqual(data, _({
    #         "name": "Fish food",
    #         "unit": u"unit",
    #         'database': 'W00t',
    #         "location": "GLO",
    #         "type": "process",
    #         "categories": ["Agricultural", "Animal production", "Animal foods"],
    #         "code": u'6524377b64855cc3daf13bd1bcfe0385',
    #         "exchanges": [{
    #             'amount': 1.0,
    #             'loc': 1.0,
    #             'input': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #             'output': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #             'type': 'production',
    #             'uncertainty type': NoUncertainty.id,
    #             'allocation': {'factor': 100.0, 'type': 'not defined'},
    #             'unit': 'unit',
    #             'folder': 'Agricultural\Animal production\Animal foods',
    #             'comment': '',
    #         }],
    #         "simapro metadata": {
    #             "Category type": "material",
    #             "Process identifier": "InsertSomethingCleverHere",
    #             "Type": "Unit process",
    #             "Process name": "bikes rule, cars drool",
    #         }
    #     }))

    # def test_production_exchange(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[])
    #     sp.importer()
    #     data = Database("W00t").load().values()[0]
    #     self.assertEqual(data['exchanges'], _([{
    #         'amount': 1.0,
    #         'loc': 1.0,
    #         'input': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #         'output': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #         'type': 'production',
    #         'uncertainty type': NoUncertainty.id,
    #         'allocation': {'factor': 100.0, 'type': 'not defined'},
    #         'unit': 'unit',
    #         'folder': 'Agricultural\Animal production\Animal foods',
    #         'comment': '',
    #     }]))

    # def test_simapro_metadata(self):
    #     sp = SimaProImporter(self.filepath("metadata"), depends=[])
    #     sp.importer()
    #     data = Database("W00t").load().values()[0]
    #     self.assertEqual(data['simapro metadata'], {
    #         "Simple": "yep!",
    #         "Multiline": ["This too", "works just fine"],
    #         "But stops": "in time"
    #     })

    # def test_linking(self):
    #     # Test number of datasets
    #     # Test internal links
    #     # Test external links with and without slashes, with and without geo
    #     database = Database("background")
    #     database.register(
    #         format="Test data",
    #     )
    #     database.write(background_data)
    #     sp = SimaProImporter(self.filepath("simple"), depends=["background"])
    #     sp.importer()

    # def test_missing(self):
    #     sp = SimaProImporter(self.filepath("missing"), depends=[])
    #     with self.assertRaises(MissingExchange):
    #         sp.importer()

    # def test_unicode_strings(self):
    #     sp = SimaProImporter(self.filepath("empty"), depends=[], default_geo=u"Where?")
    #     sp.importer()
    #     for obj in Database("W00t").load().values():
    #         for key, value in obj.items():
    #             if isinstance(key, basestring):
    #                 self.assertTrue(isinstance(key, unicode))
    #             if isinstance(value, basestring):
    #                 self.assertTrue(isinstance(value, unicode))

    # def test_comments(self):
    #     self.maxDiff = None
    #     database = Database("background")
    #     database.register()
    #     database.write(background_data)
    #     sp = SimaProImporter(self.filepath("comments"), depends=["background"])
    #     sp.importer()
    #     data = Database("W00t").load().values()[0]
    #     self.assertEqual(data['exchanges'], _([{
    #         'amount': 2.5e-10,
    #         'comment': 'single line comment',
    #         'input': ('background', "1"),
    #         'output': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #         'label': 'Materials/fuels',
    #         'loc': 2.5e-10,
    #         'location': 'CA',
    #         'name': 'lunch',
    #         'type': 'technosphere',
    #         'uncertainty': 'Lognormal',
    #         'uncertainty type': UndefinedUncertainty.id,
    #         'unit': u'kilogram'
    #     }, {
    #         'amount': 1.0,
    #         'comment': 'first line of the comment\nsecond line of the comment',
    #         'input': ('background', '2'),
    #         'output': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #         'label': 'Materials/fuels',
    #         'loc': 1.0,
    #         'location': 'CH',
    #         'name': 'dinner',
    #         'type': 'technosphere',
    #         'uncertainty': 'Lognormal',
    #         'uncertainty type': UndefinedUncertainty.id,
    #         'unit': u'kilogram'
    #     },{
    #         'amount': 1.0,
    #         'loc': 1.0,
    #         'input': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #         'output': ('W00t', '6524377b64855cc3daf13bd1bcfe0385'),
    #         'type': 'production',
    #         'uncertainty type': NoUncertainty.id,
    #         'allocation': {'factor': 100.0, 'type': 'not defined'},
    #         'unit': u'unit',
    #         'folder': 'Agricultural\Animal production\Animal foods',
    #         'comment': 'first line of comment\nsecond line of comment',
    #     }]))

# # Test multiple background DBs
