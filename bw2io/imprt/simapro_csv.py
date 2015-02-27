# -*- coding: utf-8 -*
from __future__ import print_function
from ..extractors.simapro_csv import SimaProCSVExtractor
from ..strategies import (
    link_based_on_name_and_unit,
    link_biosphere_by_activity_hash,
    normalize_simapro_biosphere,
    sp_allocate_products,
    sp_detoxify_link_external_technosphere_by_activity_hash,
    sp_match_ecoinvent3_database,
    split_simapro_name_geo,
)
from ..utils import load_json_data_file
from .base import ImportBase
from bw2data import databases, Database, config
from time import time
import copy
import functools
import warnings


class SimaProCSVImporter(ImportBase):
    format_strategies = [
        sp_allocate_products,
        link_based_on_name_and_unit,
        split_simapro_name_geo,
    ]
    format = u"SimaPro CSV"

    def __init__(self, filepath, delimiter=";", name=None, encoding='cp1252',
                 normalize_biosphere=True, biosphere_db=None):
        start = time()
        self.data = SimaProCSVExtractor.extract(filepath, delimiter, name,
                                                encoding)
        print(u"Extracted {} unallocated datasets in {:.2f} seconds".format(
              len(self.data), time() - start))
        if name:
            self.db_name = name
        else:
            self.db_name = self.get_db_name()

        if normalize_biosphere:
            self.format_strategies.append(normalize_simapro_biosphere)
        self.format_strategies.append(functools.partial(
            link_biosphere_by_activity_hash,
            biosphere_db_name=biosphere_db or config.biosphere))

    def get_db_name(self):
        candidates = {obj['database'] for obj in self.data}
        if not len(candidates) == 1:
            raise ValueError("Can't determine database name from {}".format(candidates))
        return list(candidates)[0]

    def match_ecoinvent3(self, db_name, system_model):
        """Link SimaPro transformed names to an ecoinvent 3.X database.

        Will only link processes from the given ``system_model``. Available ``system_model``s are:

            * apos
            * consequential
            * cutoff

        Matching across system models is possible, but not all processes in one system model exist in other system models.

        """
        currently_unmatched = self.statistics(False)[2]
        func_list = [functools.partial(
            sp_match_ecoinvent3_database,
            ei3_name=db_name
        )]
        self._apply_strategies(func_list)
        matched = currently_unmatched - self.statistics(False)[2]
        print(u"Matched {} exchanges".format(matched))

    def match_ecoinvent2(self, db_name):
        currently_unmatched = self.statistics(False)[2]
        func_list = [functools.partial(
            sp_detoxify_link_external_technosphere_by_activity_hash,
            external_db_name=db_name
        )]
        self._apply_strategies(func_list)
        matched = currently_unmatched - self.statistics(False)[2]
        print(u"Matched {} exchanges".format(matched))
