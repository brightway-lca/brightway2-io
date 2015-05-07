# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..units import normalize_units
from ..utils import activity_hash
from ..compatibility import SIMAPRO_BIOSPHERE, SIMAPRO_BIO_SUBCATEGORIES
from bw2data import Database, databases, config
from bw2data.logs import get_io_logger, close_log
from bw2parameters import ParameterSet
from numbers import Number
from stats_arrays import *
import os
import math
import unicodecsv
import uuid


INTRODUCTION = u"""Starting SimaPro import:
\tFilepath: %s
\tDelimiter: %s
\tName: %s
"""

SIMAPRO_TECHNOSPHERE = {
    u"Avoided products",
    u"Electricity/heat",
    u"Final waste flows",
    u"Materials/fuels",
    u"Waste to treatment",
}

SIMAPRO_PRODUCTS = {
    u"Products",
    u"Waste treatment"
}

SIMAPRO_END_OF_DATASETS = {
    u"Database Calculated parameters",
    u"Database Input parameters",
    u"Literature reference",
    u"Project Input parameters",
    u"Project Input parameters",
    u"Quantities",
    u"Units",
}


class EndOfDatasets(Exception):
    pass


def to_number(obj):
    try:
        return float(obj.replace(",", ".").strip())
    except (ValueError, SyntaxError):
        try:
            return float(eval(obj.replace(",", ".").strip()))
        except NameError:
            return obj


def filter_delete_char(fp):
    """Where does this come from? \x7f is ascii delete code..."""
    for line in open(fp):
        yield line.replace('\x7f', '')


class SimaProCSVExtractor(object):
    @classmethod
    def extract(cls, filepath, delimiter=";", name=None, encoding='cp1252'):
        assert os.path.exists(filepath), "Can't find file %s" % filepath
        log, logfile = get_io_logger(u"SimaPro-extractor")

        log.info(INTRODUCTION % (
            filepath,
            repr(delimiter),
            name,
        ))
        lines = cls.load_file(filepath, delimiter, encoding)

        # Check if valid SimaPro file
        assert u'SimaPro' in lines[0][0], "File is not valid SimaPro export"

        project_name = name or cls.get_project_name(lines)
        datasets = []

        index = cls.get_next_process_index(lines, 0)

        while True:
            try:
                ds, index = cls.read_data_set(lines, index, project_name,
                                              filepath)
                datasets.append(ds)
                index = cls.get_next_process_index(lines, index)
            except EndOfDatasets:
                break

        close_log(log)
        return datasets

    @classmethod
    def get_next_process_index(cls, data, index):
        while True:
            try:
                if data[index] and data[index][0] in SIMAPRO_END_OF_DATASETS:
                    raise EndOfDatasets
                elif data[index] and data[index][0] == u"Process":
                    return index + 1
            except IndexError:
                # File ends without extra metadata
                raise EndOfDatasets
            index += 1

    @classmethod
    def load_file(cls, filepath, delimiter, encoding):
        """Open the CSV file and load the data.

        Returns:
            The loaded data: a list of lists.

        """
        return [x for x in unicodecsv.reader(
            filter_delete_char(filepath),
            delimiter=delimiter,
            encoding=encoding,
        )]

    @classmethod
    def get_project_name(cls, data):
        for line in data[:25]:
            if u"{Project:" in line[0]:
                return line[0][9:-1].strip()

    @classmethod
    def invalid_uncertainty_data(cls, amount, kind, field1, field2, field3):
        if (kind == "Lognormal" and (not amount or field1 == "0")):
            return True

    @classmethod
    def create_distribution(cls, amount, kind, field1, field2, field3):
        amount = to_number(amount)
        if kind == "Undefined":
            return {
                u'uncertainty type': UndefinedUncertainty.id,
                u'loc': amount,
                u'amount': amount
            }
        elif cls.invalid_uncertainty_data(amount, kind, field1, field2, field3):
            # TODO: Log invalid data?
            return {
                u'uncertainty type': UndefinedUncertainty.id,
                u'loc': amount,
                u'amount': amount
            }
        elif kind == "Lognormal":
            return {
                u'uncertainty type': LognormalUncertainty.id,
                u'shape': math.log(math.sqrt(to_number(field1))),
                u'loc': math.log(abs(amount)),
                u'negative': amount < 0,
                u'amount': amount
            }
        elif kind == "Normal":
            return {
                u'uncertainty type': NormalUncertainty.id,
                u'shape': math.sqrt(to_number(field1)),
                u'loc': amount,
                u'negative': amount < 0,
                u'amount': amount
            }
        elif kind == "Triangle":
            return {
                u'uncertainty type': TriangularUncertainty.id,
                u'minimum': to_number(field2),
                u'maximum': to_number(field3),
                u'loc': amount,
                u'negative': amount < 0,
                u'amount': amount
            }
        elif kind == "Uniform":
            return {
                u'uncertainty type': UniformUncertainty.id,
                u'minimum': to_number(field2),
                u'maximum': to_number(field3),
                u'loc': amount,
                u'negative': amount < 0,
                u'amount': amount
            }
        else:
            raise ValueError(u"Unknown uncertainty type: {}".format(kind))

    @classmethod
    def parse_calculated_parameter(cls, line):
        """Parse line in `Calculated parameters` section.

        0. name
        1. formula
        2. comment

        Can include multiline comment in TSV.
        """
        return {
            u'name': line[0],
            u'formula': line[1],
            u'comment': u"; ".join([x for x in line[2:] if x])
        }

    @classmethod
    def parse_input_parameter(cls, line):
        """Parse line in `Input parameters` section.

        0. name
        1. value (not formula)
        2. uncertainty type
        3. uncert. param.
        4. uncert. param.
        5. uncert. param.
        6. hidden ("Yes" or "No" - we ignore)
        7. comment

        """
        ds = cls.create_distribution(*line[1:6])
        ds.update(**{
            u'name': line[0],
            u'comment': u"; ".join([x for x in line[7:] if x])
        })
        return ds

    @classmethod
    def parse_biosphere_flow(cls, line, category):
        """Parse biosphere flow line.

        0. name
        1. subcategory
        2. unit
        3. value or formula
        4. uncertainty type
        5. uncert. param.
        6. uncert. param.
        7. uncert. param.
        8. comment

        """
        is_formula = not isinstance(to_number(line[3]), Number)
        if is_formula:
            ds = {
                u'formula': line[3]
            }
        else:
            ds = cls.create_distribution(*line[3:8])
        ds.update(**{
            u'name': line[0],
            u'categories': (category, line[1]),
            u'unit': normalize_units(line[2]),
            u'comment': u"; ".join([x for x in line[8:] if x]),
            u'type': u'biosphere',
        })
        return ds

    @classmethod
    def parse_input_line(cls, line, category):
        """Parse technosphere input line.

        0. name
        1. unit
        2. value or formula
        3. uncertainty type
        4. uncert. param.
        5. uncert. param.
        6. uncert. param.
        7. comment

        """
        is_formula = not isinstance(to_number(line[2]), Number)
        if is_formula:
            ds = {
                u'formula': line[2]
            }
        else:
            ds = cls.create_distribution(*line[2:7])
        ds.update(**{
            u'categories': (category,),
            u'name': line[0],
            u'unit': normalize_units(line[1]),
            u'comment': u"; ".join([x for x in line[7:] if x]),
            u'type': (u"substitution" if category == "Avoided products"
                      else u'technosphere'),
        })
        return ds

    @classmethod
    def parse_reference_product(cls, line):
        """Parse reference product line.

        0. name
        1. unit
        2. value or formula
        3. allocation
        4. waste type
        5. category (separated by \\)
        6. comment

        """
        is_formula = not isinstance(to_number(line[2]), Number)
        if is_formula:
            ds = {
                u'formula': line[2]
            }
        else:
            ds = {
                u'amount': to_number(line[2])
            }
        ds.update(**{
            u'name': line[0],
            u'unit': normalize_units(line[1]),
            u'allocation': to_number(line[3]),
            u'categories': tuple(line[5].split('\\')),
            u'comment': u"; ".join([x for x in line[6:] if x]),
            u'type': u'production',
        })
        return ds

    @classmethod
    def parse_waste_treatment(cls, line):
        """Parse reference product line.

        0. name
        1. unit
        2. value or formula
        3. waste type
        4. category (separated by \\)
        5. comment

        """
        is_formula = not isinstance(to_number(line[2]), Number)
        if is_formula:
            ds = {
                u'formula': line[2]
            }
        else:
            ds = {
                u'amount': to_number(line[2])
            }
        ds.update(**{
            u'name': line[0],
            u'unit': normalize_units(line[1]),
            u'categories': tuple(line[4].split('\\')),
            u'comment': u"; ".join([x for x in line[5:] if x]),
            u'type': u'production',
        })
        return ds

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
            u'code': metadata.get(u'Process identifier') or uuid.uuid4().hex,
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
                category = data[index][0]
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
