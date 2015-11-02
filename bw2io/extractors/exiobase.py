# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from ..utils import activity_hash
from bw2data import Database, mapping, config, databases
import csv
import itertools
import os
import xlrd


class ExiobaseDataExtractor(object):
    @classmethod
    def _check_dir(cls, path):
        # Note: this assumes industry by industry
        assert os.path.isdir(path), "Must supply path to `mrIOT_IxI_fpa_coefficient_version2.2.2` folder"
        assert 'mrIot_version2.2.2.txt' in os.listdir(path), "Directory path must include Exiobase files"
        assert 'types_version2.2.2.xls' in os.listdir(path), "Directory path must include Exiobase files"

    @classmethod
    def _extract_metadata(cls, path):
        wb = xlrd.open_workbook(os.path.join(path, 'types_version2.2.2.xls'))

        ws = wb.sheet_by_name('compartments')
        compartments = [(
            int(ws.cell(row, 0).value),  # ID number
            ws.cell(row, 2).value,       # Name
        ) for row in range(1, ws.nrows)]

        ws = wb.sheet_by_name('countries')
        countries = [(
            ws.cell(row, 0).value,  # Code (2-letter ISO/other)
            ws.cell(row, 1).value,  # Name
            ws.cell(row, 3).value,  # Group code
            ws.cell(row, 4).value,  # Group name
        ) for row in range(1, ws.nrows)]

        ws = wb.sheet_by_name('industrytypes')
        industries = [(
            ws.cell(row, 0).value,  # Code
            ws.cell(row, 1).value,  # Name
            ws.cell(row, 2).value,  # Synonym
            ws.cell(row, 4).value,  # Group code
            ws.cell(row, 5).value,  # Group name
        ) for row in range(1, ws.nrows)]

        ws = wb.sheet_by_name('producttypes')
        products = [(
            ws.cell(row, 0).value,  # Code
            ws.cell(row, 1).value,  # Name
            ws.cell(row, 3).value,  # Synonym
            ws.cell(row, 4).value,  # Group code
            ws.cell(row, 5).value,  # Group name
            ws.cell(row, 7).value,  # Layer
        ) for row in range(1, ws.nrows)]

        ws = wb.sheet_by_name('units')
        units = [(
            ws.cell(row, 0).value,  # Code
            ws.cell(row, 1).value,  # Name
        ) for row in range(1, ws.nrows)]

        ws = wb.sheet_by_name('substances')
        substances = [(
            ws.cell(row, 1).value,  # Name
            ws.cell(row, 2).value,  # Code
            ws.cell(row, 3).value,  # Synonym
            ws.cell(row, 4).value,  # Description
        ) for row in range(1, ws.nrows)]

        ws = wb.sheet_by_name('extractions')
        extractions = [(
            ws.cell(row, 0).value,  # ID number
            ws.cell(row, 2).value,  # Name
            ws.cell(row, 3).value,  # Synonym
        ) for row in range(1, ws.nrows)]

        return (
            units,
            compartments,
            countries,
            industries,
            products,
            substances,
            extractions,
        )

    @classmethod
    def _extract_csv(cls, path, filename, materials=False):
        reader = csv.reader(open(os.path.join(
            path, filename)
        ), delimiter="\t")

        data = []
        countries = next(reader)[3:]
        industries = next(reader)[3:]

        for line in reader:
            for index, country, industry in zip(itertools.count(), countries,
                                               industries):
                    value = (float(line[index + 2]) if materials
                             else float(line[index + 3]))
                    if not value:
                        continue
                    elif materials:
                        data.append((
                            country,
                            industry,
                            line[0],
                            "materials",
                            line[1],
                            value
                        ))
                    else:
                        data.append((
                            country,
                            industry,
                            line[0],
                            line[1],
                            line[2],
                            value
                        ))
        return data

    @classmethod
    def _generate_csv(cls, path, filename):
        reader = csv.reader(open(os.path.join(
            path, filename)
        ), delimiter="\t")

        countries = next(reader)[3:]
        industries = next(reader)[3:]

        for line_no, line in enumerate(reader):
            for index, country, industry in zip(itertools.count(), countries,
                                               industries):
                    value = float(line[index + 3])
                    if not value:
                        continue
                    yield (
                        country,
                        industry,
                        line[0],  # country
                        line[1],  # industry
                        line[2],  # unit
                        value
                    )

    @classmethod
    def extract(cls, path):
        cls._check_dir(path)
        print("Extracting metadata")
        units, compartments, countries, industries, products, substances, \
            extractions = cls._extract_metadata(path)
        print("Extracting emissions")
        emissions = cls._extract_csv(path, "mrEmissions_version2.2.2.txt")
        # print("Extracting materials")
        # materials = cls._extract_csv(path, "mrMaterials_version2.2.2.txt", True)
        print("Extracting resources")
        resources = cls._extract_csv(path, "mrResources_version2.2.2.txt")
        print("Extracting main IO table")
        table = cls._generate_csv(path, "mrIot_version2.2.2.txt")

        outputs = {
            "compartments": compartments,
            "countries": countries,
            "emissions": emissions,
            "extractions": extractions,
            "industries": industries,
            "products": products,
            "resources": resources,
            "substances": substances,
            "table": table,
            "units": units,
        }
        return outputs
