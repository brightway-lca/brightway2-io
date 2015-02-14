# -*- coding: utf-8 -*
from __future__ import print_function
from ..units import normalize_units
from ..utils import activity_hash
from bw2data import Database, databases, config
from bw2data.logs import get_io_logger, close_log
from bw2parameters import ParameterSet
from numbers import Number
from stats_arrays import *
import os
import math
import unicodecsv


INTRODUCTION = """Starting SimaPro import:
\tFilepath: %s
\tDelimiter: %s
\tName: %s
"""

SIMAPRO_BIO_SUBCATEGORIES = {
    u"groundwater": u'ground-',
    u"groundwater, long-term": u'ground-, long-term',
    u"high. pop.": u'high population density',
    u"low. pop.": u'low population density',
    u"low. pop., long-term": u'low population density, long-term',
    u"stratosphere + troposphere": u'lower stratosphere + upper troposphere',
}

SIMAPRO_BIOSPHERE = {
    u"Economic issues": u"economic",
    u"Emissions to air": u"air",
    u"Emissions to soil": u"soil",
    u"Emissions to water": u"water",
    u"Non material emissions": u"non-material",
    u"Resources": u"resource",
    u"Social issues": u"social",
}


class EndOfDatasets(Exception):
    pass


def filter_delete_char(fp):
    """Where does this come from? \x7f is ascii delete code..."""
    for line in open(fp):
        yield line.replace('\x7f', '')


class SimaProLCIACSVExtractor(object):
    @classmethod
    def extract(cls, filepath, delimiter=";", name=None):
        assert os.path.exists(filepath), "Can't find file %s" % filepath
        log, logfile = get_io_logger(u"SimaPro-LCIA-extractor")

        log.info(INTRODUCTION % (
            filepath,
            repr(delimiter),
            name,
        ))
        lines = cls.load_file(filepath, delimiter)

        # Check if valid SimaPro file
        assert u'SimaPro' in lines[0][0], "File is not valid SimaPro export"

        datasets = []

        index = cls.get_next_method_index(lines, 0)

        while True:
            try:
                ds, index = cls.read_data_set(lines, index, filepath)
                datasets.append(ds)
                index = cls.get_next_method_index(lines, index)
            except EndOfDatasets:
                break

        close_log(log)
        return datasets

    @classmethod
    def get_next_method_index(cls, data, index):
        while True:
            try:
                if data[index] and data[index][0] == "Quantities":
                    raise EndOfDatasets
                elif data[index] and data[index][0] == u"Method":
                    return index + 1
            except IndexError:
                # File ends without extra metadata
                raise EndOfDatasets
            index += 1

    @classmethod
    def load_file(cls, filepath, delimiter):
        """Open the CSV file and load the data.

        Returns:
            The loaded data: a list of lists.

        """
        return [x for x in unicodecsv.reader(
            filter_delete_char(filepath),
            delimiter=delimiter,
            encoding="latin1",
        )]

    @classmethod
    def parse_cf(cls, line):
        """Parse line in `Substances` section.

        0. category
        1. subcategory
        2. flow
        3. CAS number
        4. CF
        5. unit

        """
        if line[1] == "(unspecified)":
            categories = (line[0].lower(),)
        else:
            categories = (line[0].lower(),
                          SIMAPRO_BIOSPHERE.get(line[1], line[1].lower()))
        return {
            u'amount': float(line[4]),
            u'CAS number': line[3],
            u'categories': categories,
            u'loc': float(line[4]),
            u'name': line[2],
            u'uncertainty type': 0,
            u'unit': normalize_units(line[5]).
        }

    @classmethod
    def read_metadata(cls, data, index):
        metadata = {}
        while True:
            if not data[index]:
                pass
            elif data[index] and data[index][0] in SIMAPRO_PRODUCTS:
                return metadata, index
            elif data[index] and data[index + 1] and data[index][0]:
                metadata[data[index][0]] = data[index + 1][0]
                index += 1
            index += 1

    @classmethod
    def read_data_set(cls, data, index, db_name, filepath):
        metadata, index = cls.read_metadata(data, index)
        # `index` is now the `Products` or `Waste Treatment` line
        ds = {
            u'simapro metadata': metadata,
            u'code': metadata[u'Process identifier'],
            u'exchanges': [],
            u'parameters': [],
            u'database': db_name,
            u'filename': filepath,
            u"type": u"process",

        }
        while not data[index] or data[index][0] != 'End':
            if not data[index] or not data[index][0]:
                index += 1
            elif data[index][0] in SIMAPRO_TECHNOSPHERE:
                category = data[index][0]
                index += 1 # Advance to data lines
                while data[index] and data[index][0]:  # Stop on blank line
                    ds[u'exchanges'].append(
                        cls.parse_input_line(data[index], category)
                    )
                    index += 1
            elif data[index][0] in SIMAPRO_BIOSPHERE:
                category = SIMAPRO_BIOSPHERE[data[index][0]]
                index += 1 # Advance to data lines
                while data[index] and data[index][0]:  # Stop on blank line
                    ds[u'exchanges'].append(
                        cls.parse_biosphere_flow(data[index], category)
                    )
                    index += 1
            elif data[index][0] == u"Calculated parameters":
                index += 1 # Advance to data lines
                while data[index] and data[index][0]:  # Stop on blank line
                    ds[u'parameters'].append(
                        cls.parse_calculated_parameter(data[index])
                    )
                    index += 1
            elif data[index][0] == u"Input parameters":
                index += 1 # Advance to data lines
                while data[index] and data[index][0]:  # Stop on blank line
                    ds[u'parameters'].append(
                        cls.parse_input_parameter(data[index])
                    )
                    index += 1
            elif data[index][0] == u"Products":
                index += 1 # Advance to data lines
                while data[index] and data[index][0]:  # Stop on blank line
                    ds[u'exchanges'].append(
                        cls.parse_reference_product(data[index])
                    )
                    index += 1
            elif data[index][0] == u"Waste treatment":
                index += 1 # Advance to data lines
                while data[index] and data[index][0]:  # Stop on blank line
                    ds[u'exchanges'].append(
                        cls.parse_waste_treatment(data[index])
                    )
                    index += 1
            elif data[index][0] in SIMAPRO_END_OF_DATASETS:
                # Don't care about processing steps below, as no dataset
                # was extracted
                raise EndOfDatasets
            else:
                index += 1

        # TODO: Adjust formulas from SP to Python?

        if ds['parameters']:
            ParameterSet(ds['parameters'])(ds)  # Changes in-place
        else:
            del ds['parameters']
        ds[u'products'] = [x for x in ds['exchanges']
                           if x['type'] == "production"]
        return ds, index
