# -*- coding: utf-8 -*
from ..units import normalize_units
from ..utils import activity_hash
from bw2data import Database, Method, methods, mapping, config
from bw2data.logs import get_io_logger, close_log
from bw2data.utils import recursive_str_to_unicode
from lxml import objectify
import os
import pprint
import progressbar
import warnings
try:
    import cPickle as pickle
except:
    import pickle


class EcospoldImpactAssessmentExtractor(object):
    """Import impact assessment methods and weightings from ecospold XML format.

Does not have any arguments; instead, instantiate the class, and then import using the ``importer`` method, i.e. ``EcospoldImpactAssessmentImporter().importer(filepath)``.

    """
    def importer(self, path):
        """Import an impact assessment method, or a directory of impact assessment methods.

        The flow logic is relatively complex, because:
            #. We have to make sure the ``number`` attribute is not just a sequential list.
            #. Even if a valid biosphere ``number`` is provided, we can't believe it.

        Here is the flow logic graphic:

        .. image:: images/import-method.png
            :align: center

        Args:
            * *path* (str): A filepath or directory.

        """
        if os.path.isdir(path):
            files = [os.path.join(path, name) for name in \
                filter(lambda x: x[-4:].lower() == ".xml", os.listdir(path))]
        else:
            files = [path]

        self.log, self.logfile = get_io_logger("lcia-import")

        try:
            self.biosphere_data = Database(config.biosphere).load()
        except:
            # Biosphere not loaded
            raise ValueError("Can't find biosphere database; check configuration.")

        if progressbar:
            widgets = [
                progressbar.SimpleProgress(sep="/"), " (",
                progressbar.Percentage(), ') ',
                progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
                progressbar.ETA()
            ]
            pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(files)
                ).start()

        for index, filepath in enumerate(files):
            # Note that this is only used for the first root method found in
            # the file
            root = objectify.parse(open(filepath)).getroot()
            for dataset in root.iterchildren():
                self.add_method(dataset)
            pbar.update(index)

        pbar.finish()

    def add_method(self, ds):
        self.new_flows = []
        ref_func = ds.metaInformation.processInformation.referenceFunction
        name = (ref_func.get("category"), ref_func.get("subCategory"),
            ref_func.get("name"))
        assert name not in methods, "%s already imported" % str(name)
        description = ref_func.get("generalComment") or ""
        unit = ref_func.get("unit") or ""
        data = [self.add_cf(o) for o in ds.flowData.iterchildren()]

        if self.new_flows:
            biosphere = Database(config.biosphere)
            biosphere_data = biosphere.load()
            # Could be considered dirty to .pop() inside list comprehension
            # but it works. The dictionary constructor also eliminates
            # duplicates.
            biosphere_data.update(recursive_str_to_unicode(
                dict([((config.biosphere, o.pop("hash")), o
                    ) for o in self.new_flows]
                )
            ))
            biosphere.write(biosphere_data)
            biosphere.process()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            method = Method(name)
            method.register(unit=unit, description=description)
            method.write(recursive_str_to_unicode([
                [(config.biosphere, o[0]), o[1], config.global_location]
                for o in data
            ]))
            method.process()

    def add_cf(self, cf):
        data = self.get_cf_data(cf)
        if (config.biosphere, data["hash"]) not in mapping:
            self.add_flow(data)
        return (data["hash"], float(cf.get("meanValue")))

    def get_cf_data(self, cf):
        data = {
            "name": cf.get("name"),
            "categories": [cf.get("category"),
                cf.get("subCategory") or "unspecified"],
            "unit": normalize_units(cf.get("unit")),
            }
        # Convert ("foo", "unspecified") to ("foo",)
        while data["categories"][-1] == "unspecified":
            data["categories"] = data["categories"][:-1]
        data["hash"] = activity_hash(data)
        return data

    def add_flow(self, cf):
        self.log.warning("Adding new biosphere flow:\n%s" % pprint.pformat(cf))
        cf.update({
            "exchanges": [],
            "type": "resource" if cf["categories"][0] == "resource" \
                else "emission"
            })
        self.new_flows.append(cf)
