# Preserved only for ideas on how to handle edge cases not yet in the codebase

# # -*- coding: utf-8 -*
# from __future__ import division, print_function
# from ..utils import activity_hash
# from .simapro_utilities import is_simapro8, SimaProMangler, NotFound
# from .units import normalize_units
# from bw2data import Database, mapping, config, databases
# from bw2data.logs import get_io_logger, close_log
# from bw2data.utils import recursive_str_to_unicode
# from lxml import objectify
# from stats_arrays.distributions import *
# import copy
# import math
# import numpy as np
# import os
# import pprint
# import progressbar
# import warnings

# BIOSPHERE = ("air", "water", "soil", "resource", "final-waste-flow")  # Waste flow from SimaPro

# widgets = [
#     progressbar.SimpleProgress(sep="/"), " (",
#     progressbar.Percentage(), ') ',
#     progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
#     progressbar.ETA()
# ]


# class Ecospold1Importer(object):
#     """Import inventory datasets from ecospold XML format.

#     Does not have any arguments; instead, instantiate the class, and then import using the ``importer`` method, i.e. ``Ecospold1Importer().importer(filepath)``.

#     """
#     def importer(self, path, name, depends=None, biosphere=config.biosphere, flavor=None, remapping={}):
#         """Import an inventory dataset, or a directory of inventory datasets.

#         .. image:: images/import-method.png
#             :align: center

#         Args:
#             *path* (str): A filepath or directory.

#         """

#         if LognormalUncertainty is None:
#             warnings.warn(u"``stats_array`` not installed!")
#             return

#         if depends is None:
#             depends = [biosphere]

#         name, path = unicode(name), unicode(path)
#         self.log, self.logfile = get_io_logger("lci-import")
#         self.new_activities = []
#         self.new_biosphere = []
#         self.remapping = remapping
#         self.biosphere = biosphere
#         self.flavor = flavor

#         data = Ecospold1DataExtractor.extract(path, self.log)
#         # XML is encoded in UTF-8, but we want unicode strings
#         data = recursive_str_to_unicode(data)
#         data = self.apply_transforms(data)
#         data = self.add_hashes(data)

#         if not data:
#             self.log.critical("No data found in XML file %s" % path)
#             warnings.warn(u"No data found in XML file %s" % path)
#             return

#         # self.biosphere_hashed = {key: activity_hash(ds) for key, ds in foo}

#         if flavor == "SimaPro8":
#             warnings.warn(u"SimaPro8 support is still experimental")
#             print("Loading databases for SimaPro match")
#             self.sp8_mangled_databases = [
#                 SimaProMangler(db_name)
#                 for db_name in depends
#                 if db_name != self.biosphere
#             ]
#             self.sp8_hashed_databases = {(db_name, activity_hash({
#                         yek: ds[yek]
#                     for yek in (u"location", u"name", u"unit")
#                     })
#                 ): key
#                 for db_name in depends
#                 for key, ds in Database(db_name).load().items()
#                 if db_name != self.biosphere
#             }
#             self.sp8_hashed_internal_database = {activity_hash({
#                 key: ds[key]
#                 for key in (u"location", u"name", u"unit")
#             }): (name, ds[u"hash"]) for ds in data}

#         pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(data)
#             ).start()

#         linked_data = []
#         for index, ds in enumerate(data):
#             linked_data.append(self.link_exchanges(ds, data, depends, name))
#             pbar.update(index)
#         pbar.finish()

#         data = linked_data + self.new_activities

#         if self.new_biosphere:
#             self.new_biosphere = dict([((config.biosphere, o.pop(u"hash")), o) \
#                 for o in self.new_biosphere])
#             biosphere = Database(config.biosphere)
#             biosphere_data = biosphere.load()
#             biosphere_data.update(self.new_biosphere)
#             biosphere.write(biosphere_data)

#         data = self.set_exchange_types(data)
#         data = self.clean_exchanges(data)
#         # Dictionary constructor eliminates duplicate activities
#         data = dict([((name, o.pop(u"hash")), o) for o in data])
#         self.write_database(name, data)

#         databases[name][u"directory"] = path
#         databases.flush()

#     def apply_transforms(self, data):
#         # Reserved for sublcasses, e.g. SimaPro import
#         # where some cleaning is necessary...
#         return data

#     def add_hashes(self, ds):
#         for o in ds:
#             o[u"hash"] = activity_hash(o)
#         return ds

#     def link_exchanges(self, ds, data, depends, name):
#         if self.sequential_exchanges(ds):
#             del ds[u"code"]
#         ds[u"exchanges"] = [self.link_exchange(exc, ds, data, depends, name
#             ) for exc in ds[u"exchanges"]]
#         return ds

#     def sequential_exchanges(self, ds):
#         codes = np.array([x[u"code"] for x in ds["exchanges"]])
#         return np.allclose(np.diff(codes), np.ones(np.diff(codes).shape))

#     def link_exchange(self, exc, ds, data, depends, name):
#         """`name`: Name of database"""
#         if exc[u"name"] in self.remapping:
#             exc[u"name"] = self.remapping[exc[u"name"]]
#         if self.flavor == "USLCI":
#             return self.link_exchange_uslci(exc, ds, data, depends, name)
#         elif self.flavor == "SimaPro8":
#             return self.link_exchange_simapro8(exc, ds, data, depends, name)
#         # Activity dataset production
#         if exc.get(u"group", None) == 0:
#             exc[u"input"] = (name, activity_hash(ds))
#             return exc
#         elif not exc[u"matching"][u"categories"]:
#             # Try to find internal link based on name
#             # Not from background database, e.g. ecoinvent, because no categories
#             for other_ds in data:
#                 if other_ds[u"name"] == \
#                         exc[u"matching"][u"name"]:
#                     exc[u"input"] = (name, other_ds[u"hash"])
#                     return exc
#             # Can't match exchange - no categories, and nothing with same name
#             # in imported database.
#             raise ValueError("Exchange can't be matched:\n%s" % \
#                 pprint.pformat(exc))
#         exc[u"hash"] = activity_hash(exc[u"matching"])
#         if exc[u"matching"].get(u"categories", [None])[0] in BIOSPHERE:
#             return self.link_biosphere(exc)
#         else:
#             return self.link_activity(exc, ds, data, depends, name)

#     def link_exchange_uslci(self, exc, ds, data, depends, name):
#         # Has to happen before others because US LCI doesn't define categories
#         # for product definitions...
#         if exc.get(u"group", None) == 0:
#             # Activity dataset production
#             exc[u"input"] = (name, activity_hash(ds))
#             return exc
#         # Hack for US LCI-specific bug - both "Energy recovered"
#         # and "Energy, recovered" are present
#         elif exc.get(u"group", None) == 1 and \
#             exc[u"matching"][u"categories"] == () and \
#                 exc[u"matching"][u"name"] == u"Recovered energy":
#             exc[u"matching"].update(
#                 name=u"Energy, recovered",
#                 categories=(u"resource",),
#                 )
#         elif not exc[u"matching"][u"categories"]:
#             # US LCI doesn't list categories, subcategories for
#             # technosphere inputs. Try to find based on name. Need to lowercase
#             # because US LCI is not consistent within itself (!!!)
#             for other_ds in data:
#                 if other_ds[u"name"].lower() == \
#                         exc[u"matching"][u"name"].lower():
#                     exc[u"input"] = (name, other_ds[u"hash"])
#                     return exc
#             # Can't find matching process - but could be a US LCI "dummy"
#             # activity
#             if exc[u"matching"][u"name"][:5].lower() == u"dummy":
#                 self.log.warning(u"New activity created by %s:\n%s" % (
#                     ds[u"filename"], pprint.pformat(exc)))
#                 exc[u"input"] = (name, self.create_activity(exc[u"matching"]))
#                 return exc
#             else:
#                 raise ValueError("Exchange can't be matched:\n%s" % \
#                     pprint.pformat(exc))
#         exc[u"hash"] = activity_hash(exc[u"matching"])
#         if exc[u"matching"].get(u"categories", [None])[0] in BIOSPHERE:
#             return self.link_biosphere(exc)
#         else:
#             return self.link_activity(exc, ds, data, depends, name)

#     def link_exchange_simapro8(self, exc, ds, data, depends, name):
#         # Activity dataset production
#         if exc.get(u"group", None) == 0:
#             exc[u"input"] = (name, activity_hash(ds))
#             return exc
#         exc[u"hash"] = activity_hash(exc[u"matching"])
#         if exc['group'] == 4:
#             assert exc[u"matching"][u"categories"][0] in BIOSPHERE, \
#                 u"Incorrect category for biosphere flow"
#             return self.link_biosphere(exc)
#         elif not exc[u"matching"][u"categories"]:
#             # SimaPro doesn't list categories, subcategories for
#             # technosphere inputs.
#             return self.link_activity_simapro8(exc, ds, data, depends, name)
#         else:
#             raise ValueError("Exchange can't be matched:\n%s" % \
#                 pprint.pformat(exc))

#     def link_biosphere(self, exc):
#         exc[u"input"] = (self.biosphere, exc[u"hash"])
#         if (self.biosphere, exc[u"hash"]) in Database(self.biosphere).load():
#             return exc
#         else:
#             new_flow = copy.deepcopy(exc[u"matching"])
#             new_flow.update({
#                 u"hash": activity_hash(exc[u"matching"]),
#                 u"type": u"resource" if new_flow[u"categories"][0] == u"resource" \
#                     else u"emission",
#                 u"exchanges": []
#                 })
#             # Biosphere flows don't have locations
#             del new_flow[u"location"]
#             self.log.warning(u"Created new biosphere flow:\n%s" % \
#                 pprint.pformat(new_flow))
#             self.new_biosphere.append(new_flow)
#             return exc

#     def link_activity(self, exc, ds, data, depends, name):
#         if exc[u"hash"] in (o[u"hash"] for o in data):
#             exc[u"input"] = (name, exc[u"hash"])
#             return exc
#         elif exc[u"hash"] in (o[u"hash"] for o in self.new_activities):
#             exc[u"input"] = (name, exc[u"hash"])
#             return exc
#         else:
#             return self.link_activity_dependent_database(exc, ds, data, depends, name)

#     def link_activity_dependent_database(self, exc, ds, data, depends, name):
#         for database in depends:
#             if (database, exc[u"hash"]) in mapping:
#                 exc[u"input"] = (database, exc[u"hash"])
#                 return exc
#         # Create new activity in this database and log
#         self.log.warning(u"New activity created by %s:\n%s" % (
#             ds[u"filename"], pprint.pformat(exc)))
#         exc[u"input"] = (name, self.create_activity(exc[u"matching"]))
#         return exc

#     def link_activity_simapro8(self, exc, ds, data, depends, name):
#         # First try in newly created activities
#         if exc[u"hash"] in (o[u"hash"] for o in self.new_activities):
#             exc[u"input"] = (name, exc[u"hash"])
#             return exc

#         if any([o in exc[u'matching'][u'location'] for o in (u"Alloc Def", u"Conseq", u"Cutoff")]):
#             # Lazy programming split location from name string, as in 2.2 mangled names
#             # Result is u'location': u'8, hot rolled {RER}| production | Alloc Def,',
#             # u'name': u'steel, chromium steel 18',
#             assert exc[u'matching'][u"location"][-1] == u","
#             exc[u'matching'][u"name"] = exc[u'matching'][u"name"] + u"/" + \
#                 exc[u'matching'][u"location"] + u" U"

#         sp8 = is_simapro8(exc[u"matching"][u'name'])
#         if sp8:
#             exc[u'matching'][u'location'] = sp8[u'location']
#             if u"(waste treatment)" in sp8[u"product"]:
#                 sp8[u"product"] = sp8[u"product"].replace("(waste treatment)", "").strip()
#                 exc[u"amount"] = -1 * exc[u"amount"]
#                 exc[u"negative"] = exc[u"amount"] < 0
#             for matcher in self.sp8_mangled_databases:
#                 try:
#                     match = matcher.match(sp8)
#                     if not Database(match[0]).load()[match][u"unit"] == \
#                             exc[u"matching"][u"unit"]:
#                         print(u"Mismatched units: %s, %s" % (exc[u"matching"][u"unit"],
#                             Database(match[0]).load()[match][u"unit"]))
#                     exc[u"input"] = match
#                     return exc
#                 except NotFound:
#                     pass
#         try:
#             # Try to match within imported database
#             exc[u'input'] = self.sp8_hashed_internal_database[exc[u"hash"]]
#             return exc
#         except KeyError:
#             # Try to match based on name, location, and unit
#             for db_name in depends:
#                 try:
#                     exc[u"input"] = self.sp8_hashed_databases[(db_name, exc[u"hash"])]
#                     return exc
#                 except KeyError:
#                     continue
#             self.log.warning(u"New activity created by %s:\n%s" % (
#                 ds[u"filename"], pprint.pformat(exc)))
#             exc[u"input"] = (name, self.create_activity(exc[u"matching"]))
#             return exc

#     def create_activity(self, exc):
#         exc = copy.deepcopy(exc)
#         exc.update({
#             u"exchanges": [],
#             u"type": u"process",
#             u"hash": activity_hash(exc),
#             })
#         self.new_activities.append(exc)
#         return exc[u"hash"]

#     def set_exchange_types(self, data):
#         """Set the ``type`` attribute for each exchange, one of either (``production``, ``technosphere``, ``biosphere``). ``production`` defines the amount produced by the activity dataset (default is 1)."""
#         for ds in data:
#             for exc in ds[u"exchanges"]:
#                 if exc[u"input"][0] == config.biosphere:
#                     exc[u"type"] = u"biosphere"
#                 elif exc["input"][1] == ds["hash"]:
#                     exc[u"type"] = u"production"
#                 else:
#                     exc[u"type"] = u"technosphere"
#         return data

#     def clean_exchanges(self, data):
#         for ds in data:
#             for exc in ds[u"exchanges"]:
#                 if u"matching" in exc:
#                     del exc[u"matching"]
#                 if u"hash" in exc:
#                     del exc[u"hash"]
#         return data

#     def write_database(self, name, data):
#         with warnings.catch_warnings():
#             warnings.simplefilter("ignore")
#             manager = Database(name)
#             manager.register(
#                 format=u"Ecospold1",
#             )
#             manager.write(data)
