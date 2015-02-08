# -*- coding: utf-8 -*
from __future__ import print_function
from ..extractors.simapro_csv import SimaProCSVExtractor
from ..strategies import (
    sp_allocate_products,
    link_based_on_name_and_unit,
    link_biosphere_by_activity_hash,
    split_simapro_name_geo,
)
from ..utils import load_json_data_file
from .base import ImportBase
from bw2data import databases, Database
from time import time

SIMAPRO_SYSTEM_MODELS = {
    "apos": "Allocation, ecoinvent default",
    "consequential": "Substitution, consequential, long-term",
    "cutoff": "Allocation, cut-off by classification",
}


class SimaProCSVImporter(ImportBase):
    format_strategies = [
        sp_allocate_products,
        link_based_on_name_and_unit,
        split_simapro_name_geo,
        link_biosphere_by_activity_hash,
    ]
    format = u"SimaPro CSV"

    def __init__(self, filepath, delimiter=";", name=None):
        start = time()
        self.data = SimaProCSVExtractor.extract(filepath, delimiter, name)
        print(u"Extracted {} unallocated datasets in {:.2f} seconds".format(
              len(self.data), time() - start))
        if name:
            self.db_name = name
        else:
            self.db_name = self.get_db_name()

    def get_db_name(self):
        candidates = {obj['database'] for obj in self.data}
        if not len(candidates) == 1:
            raise ValueError("Can't determine database name from {}".format(candidates))
        return list(candidates)[0]

    def match_ecoinvent3(self, db_name, system_model=None, debug=True):
        """Link SimaPro transformed names to an ecoinvent 3.X database.

        Will temporarily load database ``db_name`` into memory.

        If ``system_model``, will only link processes from the given system model. Available ``system_model``s are:

            * apos
            * consequential
            * cutoff

        Correspondence file is from Pré, and has the following fields:

            #. SimaPro name
            #. Ecoinvent flow name
            #. Location
            #. Ecoinvent activity name
            #. System model
            #. SimaPro type

        Note that even the official matching data from Pré is incorrect, so we have to cast all strings to lower case.

        Where system model is one of:

            * Allocation, cut-off by classification
            * Allocation, ecoinvent default

        And SimaPro type is either ``System terminated`` or ``Unit process``. We always match to unit processes regardless of SimaPro type.

        """
        to_lower = lambda x, y, z: (x.lower(), y.lower(), z.lower())
        assert db_name in databases, u"Unknown database {}".format(db_name)
        if system_model:
            try:
                system_models = set([SIMAPRO_SYSTEM_MODELS[system_model]])
            except KeyError:
                raise ValueError(u"``system_model`` must be one of: cutoff, "
                    u"consequential, apos")
        else:
            system_models = set(SIMAPRO_SYSTEM_MODELS.values())

        print(u"Loading background database: {}".format(db_name))
        possibles = {to_lower(obj['reference product'], obj.location, obj.name): obj.key for obj in Database(db_name)}
        matching_data = load_json_data_file("simapro-ecoinvent31")
        sp_mapping = {line[0]: possibles.get(to_lower(*line[1:4]))
            for line in matching_data
            if line[4] in system_models}
        count = 0
        print(u"Matching exchanges")
        for ds in self.data:
            for exc in ds.get('exchanges'):
                if exc.get('input') or not sp_mapping.get(exc['name']):
                    continue
                else:
                    exc[u'input'] = sp_mapping[exc['name']]
                    if 'unlinked' in exc:
                        del exc['unlinked']
                    count += 1
        if count:
            print(u"Matched {} exchanges".format(count))
        if debug:
            return possibles, matching_data, sp_mapping