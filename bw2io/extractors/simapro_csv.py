# -*- coding: utf-8 -*
from __future__ import print_function
from ..units import normalize_units
from ..utils import activity_hash
from bw2data import Database, databases, config
from bw2data.logs import get_io_logger
from stats_arrays import UndefinedUncertainty, NoUncertainty
import itertools
import os
import pprint
import progressbar
import re
import unicodecsv
import warnings

# Pattern for SimaPro munging of ecoinvent names
detoxify_pattern = '/(?P<geo>[A-Z]{2,10})(/I)? [SU]$'
detoxify_re = re.compile(detoxify_pattern)

widgets = [
    progressbar.SimpleProgress(sep="/"), " (",
    progressbar.Percentage(), ') ',
    progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
    progressbar.ETA()
]

class MissingExchange(StandardError):
    """Exchange can't be matched"""
    pass


def to_number(x):
    try:
        return float(x)
    except:
        return x


def detoxify(string, log):
    found = detoxify_re.findall(string)
    if not found:
        log.warning(u"Name '%s' doesn't have SimaPro slashes™ - matched without slashes" % string)
        return [string, False]

    geo = found[0][0]
    name = re.sub(detoxify_pattern, '', string)
    return [name, geo]


INTRODUCTION = """Starting SimaPro import:
\tFilepath: %s
\tDelimiter: %s
\tName: %s
\tDefault geo: %s
"""

SIMAPRO_BIOSPHERE = {
    u"Economic issues": u"economic",
    u"Emissions to air": u"air",
    u"Emissions to soil": u"soil",
    u"Emissions to water": u"water",
    u"Non material emissions": u"non-material",
    u"Resources": u"resource",
    u"Social issues": u"social",
}

SIMAPRO_BIO_SUBCATEGORIES = {
    u"high. pop.": u'high population density',
    u"low. pop.": u'low population density',
    u"low. pop., long-term": u'low population density, long-term',
    u"stratosphere + troposphere": u'lower stratosphere + upper troposphere',
    u"groundwater": u'ground-',
    u"groundwater, long-term": u'ground-, long-term',
}

SIMAPRO_TECHNOSPHERE = {
    u"Avoided products",
    u"Electricity/heat",
    u"Final waste flows",
    u"Materials/fuels",
    u"Waste to treatment",
}

SIMAPRO_PARAMETERS = {
    u"Calculated parameters",
    u"Input parameters",
}


class SimaProExtractor(object):
    """Import a SimaPro text-delimited (CSV) file into a new database.

    `SimaPro <http://www.pre-sustainability.com/simapro-lca-software>`_ is a leading commercial LCA software made by `Pré sustainbility <http://www.pre-sustainability.com/>`_.

    .. warning:: Only import of text-delimited files is supported.

    The SimaPro export must be done with **exactly** the following options checked:

    .. image:: images/simapro-options.png
        :align: center

    The basic logic of the SimaPro importer is as follows:

    .. image:: images/import-simapro.png
        :align: center

    The SimaPro importer has solid basic functionality:
        * SimaPro names are detoxified back to ecoinvent standards
        * Links to background databases like ecoinvent can be included

    However, the SimaPro importer has the following limitations:
        * Multioutput datasets are not supported.
        * Uncertainty data is not imported.
        * Social and economic flows are ignored.
        * Linking against datasets other than ecoinvent is not tested (most other databases are not publicly available in any case).
        * Modifying an existing database is not supported; it can only be overwritten completely.
        * Not all SimaPro unit changes from ecoinvent are included (e.g. where ecoinvent uses 'vehicle kilometers', simapro uses 'kilometers', making matching difficult; no comprehensive list seems to be available)
        * SimaPro unit conversions will cause problems matching to background databases (e.g. if you specify an import in megajoules, and the ecoinvent process is defined in kWh, they won't match)

    Multioutput processes could be easily supported with a bit of work; there are comments about what is needed in the source code.

    Uncertainty data could also be easily supported, but it would take someone willing to implement it.

    **Instantiation**

    Global variables:
        * ``self.db_name``: str
        * ``self.default_geo``: str
        * ``self.delimiter``: character
        * ``self.depends``: list
        * ``self.filepath``: str
        * ``self.log``: file object
        * ``self.logfile``: str
        * ``self.overwrite``: bool

    Args:
        * ``filepath``: Filepath for file to important.
        * ``delimiter`` (str, default=tab character): Delimiter character for CSV file.
        * ``depends`` (list, default= ``['ecoinvent 2.2']`` ): List of databases referenced by datasets in this file. The database *biosphere* is always matched against.
        * ``overwrite`` (bool, default= ``False`` ): Overwrite existing database.
        * ``name`` (str, default=None): Name of the database to import. If not specified, the SimaPro project name will be used.
        * ``default_geo`` (str, default= ``GLO`` ): Default location for datasets with no location is specified.
        * ``fix_missing`` (bool, default= ``False`` ): Add new processes when exchanges can't be linked. SimaPro will add so-called dummy processes, like "Waste to treatment", which don't exist in any database. If `fix_missing` is False, then these exchanges can't be linked, and an error will be raise; if it is True, then new processes in the import database will be created and linked to.

    """
    @classmethod
    def extract(cls, filepath, delimiter="\t", name=None, default_geo=u"GLO"):
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

        data, name = cls.clean_data(lines, name)
        log.info("Found %s datasets" % len(data))

        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(data)
            ).start()
        data = [cls.process_data(obj, index, pbar, log, default_geo, name)
            for index, obj in enumerate(data)]
        pbar.finish()

        return data

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
    def clean_data(cls, data, name):
        """Clean the raw data.

        1. Set the database name, if not already specified.
        2. Split lines into datasets.

        Args:
            * *data*: The raw data loaded from the CSV file.

        Returns:
            List of unlinked datasets.

        """
        if name is None:
            for line in data[:25]:
                if "{Project:" in line[0]:
                    name = line[0][9:-1].strip()
                    break
        process_indices = cls.get_process_indices(data)
        process_data = [
            data[process_indices[x]:process_indices[x + 1]]
            for x in range(len(process_indices) - 1)
        ]
        return process_data, name

    @classmethod
    def get_process_indices(cls, data):
        """Get CSV row indices for the start of each new activity dataset.

        Args:
            *data*: The CSV list of lists

        Returns:
            List of row index numbers for each dataset

        """
        return [index for index, line in enumerate(data)
            if line and line[0] == u"Process" and not "".join(line[1:])
            ] + [len(data) + 1]

    @classmethod
    def process_data(cls, dataset, index, pbar, log, default_geo, name):
        """Transform the raw dataset data to a more structured format.

        1. Add metadata like name, unit, etc.
        2. Create a list of exchanges, including the production exchange.

        Args:
            *dataset*: The raw activity dataset.

        Returns:
            Structured dataset.

        """
        data = cls.define_dataset(dataset, log, default_geo)
        data[u'simapro metadata'] = cls.get_dataset_metadata(dataset)
        data[u'exchanges'], data['parameters'] = cls.get_exchanges(dataset, log)
        data[u'exchanges'].append(cls.get_production_exchange(data, dataset, name))
        pbar.update(index)
        return data

    @classmethod
    def define_dataset(cls, dataset, log, default_geo):
        """Use the first *Products* or *Waste treatment* line to define the dataset.

        Unfortunately, all SimaPro metadata is unreliable, and can't be used.

        Args:
            *dataset*: The activity dataset.

        Returns:
            A dictionary of normal Brightway2 activity data.

        """
        line = dataset[cls.get_exchanges_index(dataset) + 1]
        name, geo = detoxify(line[0], log)
        data = {
            u'name': name,
            u'unit': normalize_units(line[2]),
            u'location': geo or default_geo,
            u'categories': line[5].split('\\'),
        }
        data[u'code'] = activity_hash(data)
        return data

    @classmethod
    def get_dataset_metadata(cls, dataset):
        """Get SimaPro-defined metadata about the dataset.

        Args:
            *dataset*: The activity dataset.

        Returns:
            A dictionary of metadata.

        """
        metadata = {}
        for index, line in enumerate(dataset):
            if line and line[0] in (u'Products', u"Waste treatment"):
                break
            elif not bool(line and len(line) > 1 and line[0] and line[1]):
                continue
            elif dataset[index + 1] and not dataset[index + 1][0]:
                # Multi-line metadata; concatenate
                metadata[line[0]] = [line[1]] + [
                    x[1] for x in itertools.takewhile(
                    lambda y: y and not y[0], dataset[index + 1:])
                ]
            else:
                metadata[line[0]] = line[1]

        return metadata

    @classmethod
    def get_exchanges(cls, dataset, log):
        """Structure the list of exchanges.

        For a normal exchange line, the fields are:
            0. Name
            1. Unit
            2. Amount
            3. Uncertainty type
            4. Uncertainty field (not sure)
            5. Uncertainty field (not sure)
            6. Uncertainty field (not sure)
            7. Comment (but can also confusingly be position 6!?)

        However, it looks like this schema could depend on the uncertainty type.

        For biosphere fields, the schema is:
            0. Name
            1. Subcategory
            2. Unit
            3. Amount or formula
            4. Uncertainty type
            5. Uncertainty field (not sure)
            6. Uncertainty field (not sure)
            7. Uncertainty field (not sure)
            8. Comment

        Args:
            *dataset*: The activity dataset.

        Returns:
            Structured list of exchanges.

        """
        exchanges, parameters = [], []
        x = cls.get_exchanges_index(dataset)
        assert cls.is_comment(dataset[x + 2]) or len(dataset[x + 2]) == 0, \
            "Can't currently import multioutput datasets"

        for index, line in enumerate(dataset[x + 3:]):
            if len(line) == 0 or cls.is_comment(line):
                continue
            elif line[0] in SIMAPRO_BIOSPHERE or line[0] in SIMAPRO_TECHNOSPHERE:
                label = line[0]
                continue
            elif line[0] == u"End":
                break

            try:
                assert len(line) > 5
                comment = line[-1]
            except:
                comment = ''

            comment += cls.get_multiline_comment(dataset, x + index + 4)

            if label in SIMAPRO_PARAMETERS:
                continue
            elif label in SIMAPRO_BIOSPHERE:
                categories = [
                    SIMAPRO_BIOSPHERE[label],
                    SIMAPRO_BIO_SUBCATEGORIES.get(line[1], line[1])
                ]
                exchanges.append({
                    u'name': line[0],
                    u'categories': filter(lambda x: x, categories),
                    u'unit': normalize_units(line[2]),
                    u'amount': to_number(line[3]),
                    u'uncertainty type': UndefinedUncertainty.id,
                    u'uncertainty': line[4],
                    u'comment': comment,
                    u'biosphere': True,
                })
            elif label == u"Final waste flows":
                name, geo = detoxify(line[0], log)

                exchanges.append({
                    u'name': name,
                    u'amount': to_number(line[2]),
                    u'uncertainty type': UndefinedUncertainty.id,
                    u'label': label,
                    u'comment': comment,
                    u'unit': normalize_units(line[3]),
                    u'uncertainty': line[4],
                    u'location': geo
                })
            else:
                # Try to interpret as ecoinvent
                name, geo = detoxify(line[0], log)

                exchanges.append({
                    u'name': name,
                    u'amount': to_number(line[2]),
                    u'uncertainty type': UndefinedUncertainty.id,
                    u'label': label,
                    u'comment': comment,
                    u'unit': normalize_units(line[1]),
                    u'uncertainty': line[3],
                    u'location': geo
                })
        return exchanges, parameters

    @classmethod
    def get_exchanges_index(cls, dataset):
        """Get index for start of exchanges in activity dataset."""
        for x in range(len(dataset)):
            if dataset[x] and dataset[x][0] in (u'Products', u"Waste treatment"):
                return x
        raise ValueError("Can't find where exchanges start for dataset")

    @classmethod
    def is_comment(cls, line):
        return (len(line) in {7,8,9}) and (''.join(line[:6]) == '')

    @classmethod
    def get_multiline_comment(cls, data, index):
        r"""Start at ``data[index]``, and consume all comment lines.

        Returns comments, with lines split with '\\n'. Returned comment starts with '\\n' because it is already the second line of a multiline comment."""
        comment = ''
        try:
            while cls.is_comment(data[index]):
                comment += (u"\n" + data[index][6])
                index += 1  # Creates new object; doesn't clobber parent index value
        except IndexError:
            pass
        return comment

    @classmethod
    def get_production_exchange(cls, data, dataset, name):
        """Get the production exchange.

        Support for multioutput processes can be added here."""
        index = cls.get_exchanges_index(dataset)
        if dataset[index][0] == u"Products":
            return cls.create_production_exchange(
                data,
                dataset,
                index + 1,
                name
            )
        elif dataset[index][0] == u"Waste treatment":
            return cls.create_waste_treatment_exchange(
                data,
                dataset,
                index + 1,
                name
            )
        else:
            raise ValueError("Can't find production exchange")

    @classmethod
    def create_production_exchange(cls, data, dataset, index, name):
        r"""For a production exchange line, the fields are:

    0. Name
    1. Unit
    2. Amount
    3. Allocation factor (out of 100)
    4. Allocation type (?)
    5. Category/subcategory, separated by '\\'
    6. Comment

        """
        line = dataset[index]
        try:
            comment = line[6]
        except:
            comment = ''
        comment += cls.get_multiline_comment(dataset, index + 1)
        return {
            u'input': (name, data[u'code']),
            u'amount': to_number(line[3]),
            u'uncertainty type': NoUncertainty.id,
            u'unit': normalize_units(line[1]),
            u'folder': line[5],
            u'comment': comment,
            u'type': u'production',
            u'allocation': {
                u'factor': float(line[3]),
                u'type': line[4]
            }
        }

    @classmethod
    def create_waste_treatment_exchange(cls, data, dataset, index, name):
        r"""For a waste treatment exchange line, the fields are:

    0. Name
    1. Amount
    2. Unit
    3. Waste types comment
    4. Category/subcategory, separated by '\\'
    5. Comment

        """
        line = dataset[index]
        try:
            comment = line[5]
        except:
            comment = ''
        comment += cls.get_multiline_comment(dataset, index + 1)
        return {
            u'input': (name, data[u'code']),
            u'amount': to_number(line[1]),
            u'uncertainty type': NoUncertainty.id,
            u'unit': normalize_units(line[2]),
            u'folder': line[4],
            u'comment': comment,
            u'type': u'production'
        }
