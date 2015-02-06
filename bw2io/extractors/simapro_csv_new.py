# -*- coding: utf-8 -*
from __future__ import print_function
from ..units import normalize_units
from ..utils import activity_hash
from bw2data import Database, databases, config
from bw2data.utils import recursive_str_to_unicode
from bw2data.logs import get_io_logger
from bw2parameters import ParameterSet
from stats_arrays import *
import itertools
import os
import pprint
import progressbar
import re
import unicodecsv
import warnings
from numbers import Number

# Pattern for SimaPro munging of ecoinvent names
detoxify_pattern = '/(?P<geo>[A-Z]{2,10})(/I)? [SU]$'
detoxify_re = re.compile(detoxify_pattern)

widgets = [
    progressbar.SimpleProgress(sep="/"), " (",
    progressbar.Percentage(), ') ',
    progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
    progressbar.ETA()
]

INTRODUCTION = """Starting SimaPro import:
\tFilepath: %s
\tDelimiter: %s
\tName: %s
\tDefault geo: %s
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
    u"System description",
    u"Units",
}


class EndOfDatasets(Exception):
    pass


def to_number(obj):
    try:
        return float(obj.replace(",", "."))
    except ValueError:
        try:
            return eval(obj.replace(",", "."))
        except ValueError:
            return obj


def detoxify(string, log):
    found = detoxify_re.findall(string)
    if not found:
        log.warning(u"Name '%s' doesn't have SimaPro slashes™ - matched without slashes" % string)
        return [string, False]

    geo = found[0][0]
    name = re.sub(detoxify_pattern, '', string)
    return [name, geo]


class SimaProExtractor(object):
    @classmethod
    def extract(cls, filepath, delimiter=";", name=None, geo=u"GLO"):
        assert os.path.exists(filepath), "Can't find file %s" % filepath
        log, logfile = get_io_logger("SimaPro-extractor")

        log.info(INTRODUCTION % (
            filepath,
            repr(delimiter),
            name,
            default_geo
        ))
        lines = cls.load_file(filepath, delimiter)

        # Check if valid SimaPro file
        assert u'SimaPro' in lines[0][0], "File is not valid SimaPro export"

        project_name = cls.get_project_name(lines)
        datasets = []

        index = cls.get_next_process_index(0)

        while True:
            try:
                ds, index = cls.read_data_set(lines, index)
                datasets.append(ds)
                index = cls.get_next_process_index(index)
            except EndOfDatasets:
                break

        # Unicode conv. not necesary if all strings in extract are unicode
        return recursive_str_to_unicode(datasets)

    @classmethod
    def load_file(cls, filepath, delimiter):
        """Open the CSV file and load the data.

        Returns:
            The loaded data: a list of lists.

        """
        return [x for x in unicodecsv.reader(
            open(filepath),
            delimiter=delimiter,
            encoding="latin1",
        )]

    @classmethod
    def get_project_name(cls, data):
        for line in data[:25]:
            if "{Project:" in line[0]:
                name = line[0][9:-1].strip()
                break
        return name

    @classmethod
    def create_distribution(cls, amount, kind, field1, field2, field3):
        amount = to_number(amount)
        if kind == "Lognormal":
            return {
                'uncertainty type': LognormalUncertainty.id,
                'shape': math.log(math.sqrt(to_number(field1))),
                'loc': math.log(abs(amount)),
                'negative': amount < 0
                'amount': amount
            }
        elif kind == "Normal":
            return {
                'uncertainty type': NormalUncertainty.id,
                'shape': math.sqrt(to_number(field1)),
                'loc': to_number(amount),
                'negative': amount < 0
                'amount': amount
            }
        elif kind == "Triangle":
            return {
                'uncertainty type': TriangularUncertainty.id,
                'minimum': to_number(field2),
                'maximum': to_number(field3),
                'loc': amount,
                'negative': amount < 0
                'amount': amount
            }
        elif kind == "Uniform":
            return {
                'uncertainty type': UniformUncertainty.id,
                'minimum': to_number(field2),
                'maximum': to_number(field3),
                'loc': amount,
                'negative': amount < 0
                'amount': amount
            }

    @classmethod
    def parse_calculated_parameter(cls, line):
        """Parse line in `Calculated parameters` section.

        0. name
        1. formula
        2. comment

        Can include multiline comment in TSV.
        """
        return {
            'name': line[0],
            'formula': line[1],
            'comment': "; ".join([x for x in line[2:] if x])
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
            'name': line[0],
            'comment': "; ".join([x for x in line[7:] if x])
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
        is_formula == isinstance(to_number(line[3]), Number):
        if is_formula:
            ds = {
                'formula': line[3]
            }
        else:
            ds = cls.create_distribution(*line[3:8])
        ds.update(**{
            'name': line[0],
            'categories': (
                category,
                SIMAPRO_BIO_SUBCATEGORIES.get(line[1], line[1])
            ),
            'unit': normalize_units(line[2]),
            'comment': "; ".join([x for x in line[8:] if x])
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
        is_formula == isinstance(to_number(line[2]), Number):
        if is_formula:
            ds = {
                'formula': line[2]
            }
        else:
            ds = cls.create_distribution(*line[2:7])
        ds.update(**{
            'categories': (category,),
            'name': line[0],
            'unit': normalize_units(line[1]),
            'comment': "; ".join([x for x in line[7:] if x])
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
        is_formula == isinstance(to_number(line[2]), Number):
        if is_formula:
            ds = {
                'formula': line[2]
            }
        else:
            ds = {
                'amount': to_number(line[2])
            }
        ds.update(**{
            'name': line[0],
            'unit': normalize_units(line[1]),
            'allocation': to_number(line[3])
            'categories': tuple(line[5].split('\\')),
            'comment': "; ".join([x for x in line[6:] if x])
        })
        return ds

    @classmethod
    def read_data_set(cls, data, index):
        metadata, index = cls.read_metadata(data, index)
        # `index` is now the `Process` or `Waste Treatment` line
        ds = {
            'simapro metadata': metadata,
            'exchanges': [],
            'parameters': [],
        }
        while data[index] != 'End':
            if data[index][0] in SIMAPRO_TECHNOSPHERE:
                category = data[index][0]
                index += 1 # Advance to data lines
                while data[index][0]:  # Stop on blank line
                    index += 1
                    ds['exchanges'].append(
                        cls.parse_input_line(data[index], category)
                    )
            elif data[index][0] in SIMAPRO_BIOSPHERE:
                category = SIMAPRO_BIOSPHERE[data[index][0]]
                index += 1 # Advance to data lines
                while data[index][0]:  # Stop on blank line
                    index += 1
                    ds['exchanges'].append(
                        cls.parse_biosphere_flow(data[index], category)
                    )
            elif data[index][0] = u"Calculated parameters":
                index += 1 # Advance to data lines
                while data[index][0]:  # Stop on blank line
                    index += 1
                    ds['parameters'].append(
                        cls.parse_calculated_parameter(data[index])
                    )
            elif data[index][0] = u"Input parameters":
                index += 1 # Advance to data lines
                while data[index][0]:  # Stop on blank line
                    index += 1
                    ds['parameters'].append(
                        cls.parse_calculated_parameter(data[index])
                    )
            elif data[index][0] in SIMAPRO_PRODUCTS:
                index += 1 # Advance to data lines
                while data[index][0]:  # Stop on blank line
                    index += 1
                    ds['exchanges'].append(
                        cls.parse_reference_product(data[index])
                    )
            elif data[index][0] in SIMAPRO_END_OF_DATASETS:
                raise EndOfDatasets
            else:
                index += 1

        # TODO: Adjust formulas from SP to Python?

        if ds['parameters']:
            ParameterSet(ds['parameters'])(ds)  # Changes in-place
        else:
            del ds['parameters']
        for exc in ds['exchanges']:
            if exc.get('category') == u"Avoided products":
                exc['amount'] *= -1
                exc['negative'] = True
        return ds, index
