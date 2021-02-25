from ..compatibility import SIMAPRO_BIOSPHERE
from ..strategies.simapro import normalize_simapro_formulae
from bw2data.logs import get_io_logger, close_log
from bw2parameters import ParameterSet
from numbers import Number
from stats_arrays import *
import csv
import math
import os
import re
import uuid


INTRODUCTION = """Starting SimaPro import:
\tFilepath: %s
\tDelimiter: %s
\tName: %s
"""

SIMAPRO_TECHNOSPHERE = {
    "Avoided products",
    "Electricity/heat",
    "Materials/fuels",
    "Waste to treatment",
}

SIMAPRO_PRODUCTS = {"Products", "Waste treatment"}

SIMAPRO_END_OF_DATASETS = {
    "Database Calculated parameters",
    "Database Input parameters",
    "Literature reference",
    "Project Input parameters",
    "Project Calculated parameters",
    "Quantities",
    "Units",
}


class EndOfDatasets(Exception):
    pass


def to_number(obj):
    try:
        return float(obj.replace(",", ".").strip())
    except (ValueError, SyntaxError):
        # Sometimes allocation or ref product specific as percentage
        if "%" in obj:
            return float(obj.replace("%", "").strip()) / 100.0
        try:
            # Eval for simple expressions like "1/2"
            return float(eval(obj.replace(",", ".").strip()))
        except NameError:
            # Formula with a variable which isn't in scope - raises NameError
            return obj
        except SyntaxError:
            # Unit string like "ha a" raises a syntax error when evaled
            return obj


# \x7f if ascii delete - where does it come from?
strip_whitespace_and_delete = (
    lambda obj: obj.replace("\x7f", "").strip() if isinstance(obj, str) else obj
)

lowercase_expression = (
    "(?:"  # Don't capture this group
    "^"  # Match the beginning of the string
    "|"  # Or
    "[^a-zA-Z_])"  # Anything other than a letter or underscore. SimaPro is limited to ASCII characters
    "(?P<variable>{})"  # The variable name string will be substituted here
    "(?:[^a-zA-Z_]|$)"  # Match anything other than a letter or underscore, or the end of the line
)


def replace_with_lowercase(string, names):
    """Replace all occurrences of elements of ``names`` in ``string`` with their lowercase equivalents.

    ``names`` is a list of variable name strings that should already all be lowercase.

    Returns a modified ``string``."""
    for name in names:
        expression = lowercase_expression.format(name)
        for result in re.findall(expression, string, re.IGNORECASE):
            if result != name:
                string = string.replace(result, result.lower())
    return string


class SimaProCSVExtractor(object):
    @classmethod
    def extract(cls, filepath, delimiter=";", name=None, encoding="cp1252"):
        assert os.path.exists(filepath), "Can't find file %s" % filepath
        log, logfile = get_io_logger("SimaPro-extractor")

        log.info(INTRODUCTION % (filepath, repr(delimiter), name,))
        with open(filepath, "r", encoding=encoding) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            lines = [
                [strip_whitespace_and_delete(obj) for obj in line] for line in reader
            ]

        # Check if valid SimaPro file
        assert (
            "SimaPro" in lines[0][0] or "CSV separator" in lines[0][0]
        ), "File is not valid SimaPro export"

        project_name = name or cls.get_project_name(lines)
        datasets = []

        project_metadata = cls.get_project_metadata(lines)
        global_parameters = cls.get_global_parameters(lines, project_metadata)

        index = cls.get_next_process_index(lines, 0)

        while True:
            try:
                ds, index = cls.read_data_set(
                    lines,
                    index,
                    project_name,
                    filepath,
                    global_parameters,
                    project_metadata,
                )
                datasets.append(ds)
                index = cls.get_next_process_index(lines, index)
            except EndOfDatasets:
                break

        close_log(log)
        return datasets, global_parameters, project_metadata

    @classmethod
    def get_next_process_index(cls, data, index):
        while True:
            try:
                if data[index] and data[index][0] in SIMAPRO_END_OF_DATASETS:
                    raise EndOfDatasets
                elif data[index] and data[index][0] == "Process":
                    return index + 1
            except IndexError:
                # File ends without extra metadata
                raise EndOfDatasets
            index += 1

    @classmethod
    def get_project_metadata(cls, data):
        meta = {}
        for line in data:
            if not line:
                return meta
            elif ":" not in line[0]:
                continue
            if not len(line) == 1:
                raise ValueError("Can't understand metadata line {}".format(line))
            assert line[0][0] == "{" and line[0][-1] == "}"
            line = line[0][1:-1].split(":")
            key, value = line[0], ":".join(line[1:])
            meta[key.strip()] = value.strip()

    @classmethod
    def get_global_parameters(cls, data, pm):
        current, parameters = None, []
        for line in data:
            if not line:  # Blank line, end of section
                current = None
            elif line[0] in {"Database Input parameters", "Project Input parameters"}:
                current = "input"
            elif line[0] in {
                "Database Calculated parameters",
                "Project Calculated parameters",
            }:
                current = "calculated"
            elif current is None:
                continue
            elif current == "input":
                parameters.append(cls.parse_input_parameter(line))
            elif current == "calculated":
                parameters.append(cls.parse_calculated_parameter(line, pm))
            else:
                raise ValueError("This should never happen")

        # Extract name and lowercase
        parameters = {obj.pop("name").lower(): obj for obj in parameters}
        # Change all formula values to lowercase if referencing global parameters
        for obj in parameters.values():
            if "formula" in obj:
                obj["formula"] = replace_with_lowercase(obj["formula"], parameters)

        ParameterSet(parameters).evaluate_and_set_amount_field()
        return parameters

    @classmethod
    def get_project_name(cls, data):
        for line in data[:25]:
            if not line:
                continue
            elif "{Project:" in line[0]:
                return line[0][9:-1].strip()
            # What the holy noodly appendage
            # All other metadata in English, only this term
            # translated into French‽
            elif "{Projet:" in line[0]:
                return line[0][9:-1].strip()

    @classmethod
    def invalid_uncertainty_data(cls, amount, kind, field1, field2, field3):
        if kind == "Lognormal" and (not amount or field1 == "0"):
            return True

    @classmethod
    def create_distribution(cls, amount, kind, field1, field2, field3):
        amount = to_number(amount)
        if kind == "Undefined":
            return {
                "uncertainty type": UndefinedUncertainty.id,
                "loc": amount,
                "amount": amount,
            }
        elif cls.invalid_uncertainty_data(amount, kind, field1, field2, field3):
            # TODO: Log invalid data?
            return {
                "uncertainty type": UndefinedUncertainty.id,
                "loc": amount,
                "amount": amount,
            }
        elif kind == "Lognormal":
            return {
                "uncertainty type": LognormalUncertainty.id,
                "scale": math.log(math.sqrt(to_number(field1))),
                "loc": math.log(abs(amount)),
                "negative": amount < 0,
                "amount": amount,
            }
        elif kind == "Normal":
            return {
                "uncertainty type": NormalUncertainty.id,
                "scale": math.sqrt(to_number(field1)),
                "loc": amount,
                "negative": amount < 0,
                "amount": amount,
            }
        elif kind == "Triangle":
            return {
                "uncertainty type": TriangularUncertainty.id,
                "minimum": to_number(field2),
                "maximum": to_number(field3),
                "loc": amount,
                "negative": amount < 0,
                "amount": amount,
            }
        elif kind == "Uniform":
            return {
                "uncertainty type": UniformUncertainty.id,
                "minimum": to_number(field2),
                "maximum": to_number(field3),
                "loc": amount,
                "negative": amount < 0,
                "amount": amount,
            }
        else:
            raise ValueError("Unknown uncertainty type: {}".format(kind))

    @classmethod
    def parse_calculated_parameter(cls, line, pm):
        """Parse line in `Calculated parameters` section.

        0. name
        1. formula
        2. comment

        Can include multiline comment in TSV.
        """
        return {
            "name": line[0],
            "formula": normalize_simapro_formulae(line[1], pm),
            "comment": "; ".join([x for x in line[2:] if x]),
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
        ds.update({"name": line[0], "comment": "; ".join([x for x in line[7:] if x])})
        return ds

    @classmethod
    def parse_biosphere_flow(cls, line, category, pm):
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

        However, sometimes the value is in index 2, and the unit in index 3. Because why not! We assume default ordering unless we find a number in index 2.

        """
        unit, amount = line[2], line[3]
        if isinstance(to_number(line[2]), Number):
            unit, amount = amount, unit

        is_formula = not isinstance(to_number(amount), Number)
        if is_formula:
            ds = {"formula": normalize_simapro_formulae(amount, pm)}
        else:
            ds = cls.create_distribution(amount, *line[4:8])
        ds.update(
            {
                "name": line[0],
                "categories": (category, line[1]),
                "unit": unit,
                "comment": "; ".join([x for x in line[8:] if x]),
                "type": "biosphere",
            }
        )
        return ds

    @classmethod
    def parse_input_line(cls, line, category, pm):
        """Parse technosphere input line.

        0. name
        1. unit
        2. value or formula
        3. uncertainty type
        4. uncert. param.
        5. uncert. param.
        6. uncert. param.
        7. comment

        However, sometimes the value is in index 1, and the unit in index 2. Because why not! We assume default ordering unless we find a number in index 1.

        """
        unit, amount = line[1], line[2]
        if isinstance(to_number(line[1]), Number):
            unit, amount = amount, unit

        is_formula = not isinstance(to_number(amount), Number)
        if is_formula:
            ds = {"formula": normalize_simapro_formulae(amount, pm)}
        else:
            ds = cls.create_distribution(amount, *line[3:7])
        ds.update(
            {
                "categories": (category,),
                "name": line[0],
                "unit": unit,
                "comment": "; ".join([x for x in line[7:] if x]),
                "type": (
                    "substitution" if category == "Avoided products" else "technosphere"
                ),
            }
        )
        return ds

    @classmethod
    def parse_final_waste_flow(cls, line, pm):
        """Parse final wate flow line.

        0: name
        1: subcategory?
        2: unit
        3. value or formula
        4. uncertainty type
        5. uncert. param.
        6. uncert. param.
        7. uncert. param.

        However, sometimes the value is in index 2, and the unit in index 3. Because why not! We assume default ordering unless we find a number in index 2.

        """
        unit, amount = line[2], line[3]
        if isinstance(to_number(line[2]), Number):
            unit, amount = amount, unit

        is_formula = not isinstance(to_number(amount), Number)
        if is_formula:
            ds = {"formula": normalize_simapro_formulae(amount, pm)}
        else:
            ds = cls.create_distribution(amount, *line[4:8])
        ds.update(
            {
                "name": line[0],
                "categories": ("Final waste flows", line[1])
                if line[1]
                else ("Final waste flows",),
                "unit": unit,
                "comment": "; ".join([x for x in line[8:] if x]),
                "type": "technosphere",
            }
        )
        return ds

    @classmethod
    def parse_reference_product(cls, line, pm):
        """Parse reference product line.

        0. name
        1. unit
        2. value or formula
        3. allocation
        4. waste type
        5. category (separated by \\)
        6. comment

        However, sometimes the value is in index 1, and the unit in index 2. Because why not! We assume default ordering unless we find a number in index 1.

        """
        unit, amount = line[1], line[2]
        if isinstance(to_number(line[1]), Number):
            unit, amount = amount, unit

        is_formula = not isinstance(to_number(amount), Number)
        if is_formula:
            ds = {"formula": normalize_simapro_formulae(amount, pm)}
        else:
            ds = {"amount": to_number(amount)}
        ds.update(
            {
                "name": line[0],
                "unit": unit,
                "allocation": to_number(line[3]),
                "categories": tuple(line[5].split("\\")),
                "comment": "; ".join([x for x in line[6:] if x]),
                "type": "production",
            }
        )
        return ds

    @classmethod
    def parse_waste_treatment(cls, line, pm):
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
            ds = {"formula": normalize_simapro_formulae(line[2], pm)}
        else:
            ds = {"amount": to_number(line[2])}
        ds.update(
            {
                "name": line[0],
                "unit": line[1],
                "categories": tuple(line[4].split("\\")),
                "comment": "; ".join([x for x in line[5:] if x]),
                "type": "production",
            }
        )
        return ds

    @classmethod
    def read_dataset_metadata(cls, data, index):
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
    def read_data_set(cls, data, index, db_name, filepath, gp, pm):
        metadata, index = cls.read_dataset_metadata(data, index)
        # `index` is now the `Products` or `Waste Treatment` line
        ds = {
            "simapro metadata": metadata,
            "code": metadata.get("Process identifier") or uuid.uuid4().hex,
            "exchanges": [],
            "parameters": [],
            "database": db_name,
            "filename": filepath,
            "type": "process",
        }
        while not data[index] or data[index][0] != "End":
            if not data[index] or not data[index][0]:
                index += 1
            elif data[index][0] in SIMAPRO_TECHNOSPHERE:
                category = data[index][0]
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["exchanges"].append(
                        cls.parse_input_line(data[index], category, pm)
                    )
                    index += 1
            elif data[index][0] in SIMAPRO_BIOSPHERE:
                category = data[index][0]
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["exchanges"].append(
                        cls.parse_biosphere_flow(data[index], category, pm)
                    )
                    index += 1
            elif data[index][0] == "Calculated parameters":
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["parameters"].append(
                        cls.parse_calculated_parameter(data[index], pm)
                    )
                    index += 1
            elif data[index][0] == "Input parameters":
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["parameters"].append(cls.parse_input_parameter(data[index]))
                    index += 1
            elif data[index][0] == "Products":
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["exchanges"].append(cls.parse_reference_product(data[index], pm))
                    index += 1
            elif data[index][0] == "Waste treatment":
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["exchanges"].append(cls.parse_waste_treatment(data[index], pm))
                    index += 1
            elif data[index][0] == "Final waste flows":
                index += 1  # Advance to data lines
                while (
                    index < len(data) and data[index] and data[index][0]
                ):  # Stop on blank line
                    ds["exchanges"].append(cls.parse_final_waste_flow(data[index], pm))
                    index += 1
            elif data[index][0] in SIMAPRO_END_OF_DATASETS:
                # Don't care about processing steps below, as no dataset
                # was extracted
                raise EndOfDatasets
            else:
                index += 1

            if index == len(data):
                break

        # Extract name and lowercase
        ds["parameters"] = {obj.pop("name").lower(): obj for obj in ds["parameters"]}

        # Change all parameter formula values to lowercase if referencing
        # global or local parameters
        for obj in ds["parameters"].values():
            if "formula" in obj:
                obj["formula"] = replace_with_lowercase(
                    obj["formula"], ds["parameters"]
                )
                obj["formula"] = replace_with_lowercase(obj["formula"], gp)
        # Change all exchange values to lowercase if referencing
        # global or local parameters
        for obj in ds["exchanges"]:
            if "formula" in obj:
                obj["formula"] = replace_with_lowercase(
                    obj["formula"], ds["parameters"]
                )
                obj["formula"] = replace_with_lowercase(obj["formula"], gp)

        ps = ParameterSet(
            ds["parameters"], {key: value["amount"] for key, value in gp.items()}
        )
        # Changes in-place
        ps(ds["exchanges"])

        if not ds["parameters"]:
            del ds["parameters"]

        return ds, index
