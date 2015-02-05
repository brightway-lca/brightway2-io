# -*- coding: utf-8 -*
from __future__ import print_function
from .units import normalize_units
from bw2data import Database, databases, config
from bw2data.logs import get_io_logger
from bw2data.utils import activity_hash
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
\tDepends: %s
\tName: %s
\tDefault geo: %s
"""

SIMAPRO_BIOSPHERE = {
    u"Emissions to air": u"air",
    u"Resources": u"resource",
    u"Emissions to water": u"water",
    u"Emissions to soil": u"soil",
}

SIMAPRO_BIO_SUBCATEGORIES = {
    u"high. pop.": u'high population density',
    u"low. pop.": u'low population density',
    u"low. pop., long-term": u'low population density, long-term',
    u"stratosphere + troposphere": u'lower stratosphere + upper troposphere',
    u"groundwater": u'ground-',
    u"groundwater, long-term": u'ground-, long-term',
}


class SimaProImporter(object):
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
    def __init__(self, filepath, delimiter="\t", depends=['ecoinvent 2.2'],
                 overwrite=False, name=None, default_geo=u"GLO",
                 fix_missing=False):
        assert os.path.exists(filepath), "Can't find file %s" % filepath
        self.filepath = filepath
        self.delimiter = delimiter
        self.depends = depends
        self.overwrite = overwrite
        self.db_name = name
        self.default_geo = default_geo
        self.fix_missing = fix_missing
        self.log, self.logfile = get_io_logger("SimaPro-importer")

    def importer(self):
        """Import the SimaPro file."""
        self.log.info(INTRODUCTION % (
            self.filepath,
            repr(self.delimiter),
            ", ".join(self.depends),
            self.db_name,
            self.default_geo
        ))
        data = self.load_file()
        self.verify_simapro_file(data)
        format = data[0][0]
        data = self.clean_data(data)
        self.log.info("Found %s datasets" % len(data))

        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(data)
            ).start()
        data = [self.process_data(obj, index, pbar)
            for index, obj in enumerate(data)]
        pbar.finish()

        self.create_foreground(data)
        self.load_background()
        self.new_processes = []

        pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(data)
            ).start()
        data = [self.link_exchanges(obj, index, pbar) for index, obj in enumerate(data)] + self.new_processes
        pbar.finish()

        if self.overwrite:
            with warnings.catch_warnings():
                database = Database(self.db_name)

            if self.db_name in databases:
                self.log.warning("Overwriting database %s" % self.db_name)
            else:
                database.register(
                    format=format,
                )
        else:
            assert self.db_name not in databases, (
                "Already imported this project\n"
                "Delete existing database, give new name, or use ``overwrite``."
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                database = Database(self.db_name)
                database.register(
                    format=format,
                )
        database.write(dict([
            ((self.db_name, obj['code']), obj) for obj in data
        ]))
        database.process()

        print("SimaPro file imported successfully. Please check the logfile:\n\t" + self.logfile)

        return self.db_name, self.logfile

    def load_file(self):
        """Open the CSV file and load the data.

        Returns:
            The loaded data: a list of lists.

        """
        return [x for x in unicodecsv.reader(
            open(self.filepath),
            delimiter=self.delimiter,
            encoding="latin1",
        )]

    def verify_simapro_file(self, data):
        """Check to make sure file is valid SimaPro export.

        Args:
            *data*: The raw data loaded from the CSV file.

        """
        assert u'SimaPro' in data[0][0], "File is not valid SimaPro export"

    def clean_data(self, data):
        """Clean the raw data.

        1. Set the database name, if not already specified.
        2. Split datasets.

        Args:
            * *data*: The raw data loaded from the CSV file.

        Returns:
            Cleaned data.

        """
        if self.db_name is None:
            assert data[1][0] == u'Project', "Can't determine SimaPro project name"
            self.db_name = data[1][1]
        process_indices = self.get_process_indices(data)
        process_data = [
            data[process_indices[x]:process_indices[x + 1]]
            for x in range(len(process_indices) - 1)
        ]
        return process_data

    def get_process_indices(self, data):
        """Get CSV row indices for the start of each new activity dataset.

        Args:
            *data*: The CSV list of lists

        Returns:
            List of row index numbers

        """
        return [x for x in range(2, len(data))
            if data[x] and data[x][0] == u"Process" and len(data[x]) == 1
            ] + [len(data) + 1]

    def process_data(self, dataset, index, pbar):
        """Transform the raw dataset data to a more structured format.

        1. Add metadata like name, unit, etc.
        2. Create a list of exchanges, including the production exchange.

        Args:
            *dataset*: The raw activity dataset.

        Returns:
            Structured dataset.

        """
        data = self.define_dataset(dataset)
        data[u'simapro metadata'] = self.get_dataset_metadata(dataset)
        data[u'exchanges'] = self.get_exchanges(dataset)
        data[u'exchanges'].append(self.get_production_exchange(data, dataset))
        pbar.update(index)
        return data

    def define_dataset(self, dataset):
        """Use the first *Products* or *Waste treatment* line to define the dataset.

        Unfortunately, all SimaPro metadata is unreliable, and can't be used.

        Args:
            *dataset*: The activity dataset.

        Returns:
            A dictionary of normal Brightway2 activity data.

        """
        line = dataset[self.get_exchanges_index(dataset) + 1]
        name, geo = detoxify(line[0], self.log)
        data = {
            u'name': name,
            u'unit': normalize_units(line[2]),
            u'location': geo or self.default_geo,
            u'categories': line[5].split('\\'),
            u'type': u'process',
        }
        data[u'code'] = activity_hash(data)
        return data

    def create_missing_dataset(self, exc):
        """Create new dataset from unlinked exchange."""
        data = {
            u'name': exc[u'name'],
            u'unit': normalize_units(exc[u'unit']),
            u'location': exc[u'location'] or self.default_geo,
            u'categories': [exc[u'label']],
            u'type': u'process',
            u'comment': exc[u'comment'],
            u'exchanges': [],
        }
        self.log.warning(u"Created new process for unlinked exchange:\n%s" \
            % pprint.pformat(exc, indent=4)
        )
        data[u'code'] = activity_hash(data)
        return data

    def get_dataset_metadata(self, dataset):
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

    def get_exchanges(self, dataset):
        """Structure the list of exchanges.

        For a normal exchange line, the fields are:
            0. Name
            1. Amount
            2. Unit
            3. Uncertainty type
            4. Uncertainty field (not sure)
            5. Uncertainty field (not sure)
            6. Uncertainty field (not sure)
            7. Comment (but can also confusingly be position 6!?)

        However, it looks like this schema could depend on the uncertainty type.

        For biosphere fields, the schema is:
            0. Name
            1. Category
            2. Unit
            3. Amount
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
        exchanges = []
        x = self.get_exchanges_index(dataset)
        assert self.is_comment(dataset[x + 2]) or len(dataset[x + 2]) == 0, \
            "Can't currently import multioutput datasets"

        for index, line in enumerate(dataset[x + 3:]):
            if len(line) == 0 or self.is_comment(line):
                continue
            elif len(line) == 1:
                label = line[0]
                continue

            try:
                assert len(line) > 5
                comment = line[-1]
            except:
                comment = ''

            comment += self.get_multiline_comment(dataset, x + index + 4)

            if label in SIMAPRO_BIOSPHERE:
                categories = [
                    SIMAPRO_BIOSPHERE[label],
                    SIMAPRO_BIO_SUBCATEGORIES.get(line[1], line[1])
                ]
                exchanges.append({
                    u'name': line[0],
                    u'categories': filter(lambda x: x, categories),
                    u'unit': normalize_units(line[2]),
                    u'amount': float(line[3]),
                    u'loc': float(line[3]),
                    u'uncertainty type': UndefinedUncertainty.id,
                    u'uncertainty': line[4],
                    u'comment': comment,
                    u'biosphere': True,
                })
            elif label ==u"Final waste flows":
                name, geo = detoxify(line[0], self.log)

                exchanges.append({
                    u'name': name,
                    u'amount': float(line[2]),
                    u'loc': float(line[2]),
                    u'uncertainty type': UndefinedUncertainty.id,
                    u'label': label,
                    u'comment': comment,
                    u'unit': normalize_units(line[3]),
                    u'uncertainty': line[4],
                    u'location': geo
                })
            else:
                # Try to interpret as ecoinvent
                name, geo = detoxify(line[0], self.log)

                exchanges.append({
                    u'name': name,
                    u'amount': float(line[1]),
                    u'loc': float(line[1]),
                    u'uncertainty type': UndefinedUncertainty.id,
                    u'label': label,
                    u'comment': comment,
                    u'unit': normalize_units(line[2]),
                    u'uncertainty': line[3],
                    u'location': geo
                })
        return exchanges

    def get_exchanges_index(self, dataset):
        """Get index for start of exchanges in activity dataset."""
        for x in range(len(dataset)):
            if dataset[x] and dataset[x][0] in (u'Products', u"Waste treatment"):
                return x
        raise ValueError("Can't find where exchanges start for dataset")

    def is_comment(self, line):
        return (len(line) in {7,8,9}) and (''.join(line[:6]) == '')

    def get_multiline_comment(self, data, index):
        r"""Start at ``data[index]``, and consume all comment lines.

        Returns comments, with lines split with '\\n'. Returned comment starts with '\\n' because it is already the second line of a multiline comment."""
        comment = ''
        try:
            while self.is_comment(data[index]):
                comment += (u"\n" + data[index][6])
                index += 1  # Creates new object; doesn't clobber parent index value
        except IndexError:
            pass
        return comment

    def get_production_exchange(self, data, dataset):
        """Get the production exchange.

        Support for multioutput processes can be added here."""
        index = self.get_exchanges_index(dataset)
        if dataset[index][0] == u"Products":
            return self.create_production_exchange(
                data,
                dataset,
                index + 1
            )
        elif dataset[index][0] == u"Waste treatment":
            return self.create_waste_treatment_exchange(
                data,
                dataset,
                index + 1
            )
        else:
            raise ValueError("Can't find production exchange")

    def create_production_exchange(self, data, dataset, index):
        r"""For a production exchange line, the fields are:

    0. Name
    1. Amount
    2. Unit
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
        comment += self.get_multiline_comment(dataset, index + 1)
        return {
            u'input': (self.db_name, data[u'code']),
            u'amount': float(line[1]),
            u'loc': float(line[1]),
            u'uncertainty type': NoUncertainty.id,
            u'unit': normalize_units(line[2]),
            u'folder': line[5],
            u'comment': comment,
            u'type': u'production',
            u'allocation': {
                u'factor': float(line[3]),
                u'type': line[4]
            }
        }

    def create_waste_treatment_exchange(self, data, dataset, index):
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
        comment += self.get_multiline_comment(dataset, index + 1)
        return {
            u'input': (self.db_name, data[u'code']),
            u'amount': float(line[1]),
            u'loc': float(line[1]),
            u'uncertainty type': NoUncertainty.id,
            u'unit': normalize_units(line[2]),
            u'folder': line[4],
            u'comment': comment,
            u'type': u'production'
        }

    def create_foreground(self, data):
        """Create the set of foreground processes to match exchanges against.

        Global variables:
            * ``self.foreground``: dict

        Args:
            *data*: The structured activity datasets.

        """
        self.foreground = dict([
            ((ds[u'name'], ds[u'unit']), (self.db_name, ds[u'code']))
            for ds in data
        ])

    def load_background(self):
        """Load the background data to match exchanges against.

        Need to be able to match against both ``(name, unit, geo)`` and ``(name, unit)``.

        Also loads the *biosphere* database.

        Global variables:
            * ``self.background``: dict
            * ``self.biosphere``: dict

        """
        background_data = {}
        for db in self.depends:
            background_data.update(**Database(db).load())

        self.background = {}
        for key, value in background_data.iteritems():
            self.background[(value[u'name'].lower(), value[u'unit'],
                            value.get(u'location', self.default_geo))] = key
            self.background[(value[u'name'].lower(), value[u'unit'])] = key

        self.biosphere = Database(config.biosphere).load()

    def link_exchanges(self, dataset, index, pbar):
        """Link all exchanges in a given dataset"""
        dataset[u'exchanges'] = [
            self.link_exchange(exc) for exc in dataset[u'exchanges']
        ]
        pbar.update(index)
        return dataset

    def link_exchange(self, exc):
        """Try to link an exchange.

        This isn't easy, as SimaPro only gives us names and units, and often the names are incorrect.

        This method looks first in the foreground, then the background; if an exchange isn't found an error is raised."""
        if exc.get('type', None) == 'production':
            return exc
        elif exc.get(u'biosphere', False):
            try:
                code = (u'biosphere', activity_hash(exc))
                assert code in self.biosphere
                exc[u'input'] = code
                exc[u'type'] = u'biosphere'
                exc[u'uncertainty type'] = UndefinedUncertainty.id
                del exc[u'biosphere']
                return exc
            except:
                raise MissingExchange(u"Can't find biosphere flow\n%s" % \
                    pprint.pformat(exc, indent=4))
        elif (exc[u"name"], exc[u"unit"]) in self.foreground:
            exc[u"input"] = self.foreground[(exc[u"name"], exc[u"unit"])]
            found = True
        elif (exc[u"name"].lower(), exc[u"unit"], exc[u'location']) in \
                self.background:
            exc[u"input"] = self.background[(exc[u"name"].lower(), exc[u"unit"],
                exc[u'location'])]
            found = True
        elif (exc[u"name"].lower(), exc[u"unit"]) in self.background:
            exc[u"input"] = self.background[(exc[u"name"].lower(), exc[u"unit"])]
            found = True
        else:
            found = False

        if found:
            exc[u"type"] = u"technosphere"
            exc[u'uncertainty type'] = UndefinedUncertainty.id
            return exc
        elif self.fix_missing:
            new_process = self.create_missing_dataset(exc)
            exc[u'type'] = u"technosphere"
            exc[u'uncertainty type'] = UndefinedUncertainty.id
            exc[u'input'] = (self.db_name, new_process[u'code'])
            self.new_processes.append(new_process)
            return exc
        else:
            raise MissingExchange(
                "Can't find exchange\n%s" % pprint.pformat(exc, indent=4)
            )
