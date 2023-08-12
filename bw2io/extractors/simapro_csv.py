import csv
import math
import os
import re
import uuid
from bw2data.logs import close_log, get_io_logger
from bw2parameters import ParameterSet
from bw2parameters.errors import MissingName
from numbers import Number
from stats_arrays import (
    LognormalUncertainty,
    NormalUncertainty,
    TriangularUncertainty,
    UndefinedUncertainty,
    UniformUncertainty,
)

from ..compatibility import SIMAPRO_BIOSPHERE
from ..strategies.simapro import normalize_simapro_formulae

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
    """Raise exception when there are no more datasets to iterate."""

    pass


def to_number(obj):
    """
    Convert a string to a number.

    Parameters
    ----------
    obj : str
        The string to be converted to a number

    Returns
    -------
    float or str
        converted number as float, or the unchanged string if not successfully converted.

    """
    try:
        return float(obj.replace(",", ".").strip())
    except (ValueError, SyntaxError):
        # Sometimes allocation or ref product specific as percentage
        if "%" in obj:
            return float(obj.replace("%", "").strip()) / 100.0
        try:
            # Eval for simple expressions like "1/2" or "10^6"
            return float(
                ParameterSet({})
                .get_interpreter()
                .eval(obj.replace(",", ".").replace("^", "**").strip())
            )
        except MissingName:
            # Formula with a variable which isn't in scope - raises NameError
            return obj
        except SyntaxError:
            # Unit string like "ha a" raises a syntax error when evaled
            return obj
        except TypeError:
            # Formulas with parameters or units that are Python built-in function like "min" (can be a parameter or a unit) raises TypeError
            return obj


# \x7f if ascii delete - where does it come from?
strip_whitespace_and_delete = (
    lambda obj: obj.replace("\x7f", "").strip() if isinstance(obj, str) else obj
)

uppercase_expression = (
    "(?:"  # Don't capture this group
    "^"  # Match the beginning of the string
    "|"  # Or
    "[^a-zA-Z_])"  # Anything other than a letter or underscore. SimaPro is limited to ASCII characters
    "(?P<variable>{})"  # The variable name string will be substituted here
    "(?:[^a-zA-Z_]|$)"  # Match anything other than a letter or underscore, or the end of the line
)


def replace_with_uppercase(string, names, precompiled):
    """
    Replace all occurrences of elements of ``names`` in ``string`` with their uppercase equivalents.

    Parameters
    ----------
    string : str
        String to be modified.
    names : list
        List of variable name strings that should already all be uppercase.
    precompiled : dict
        Dictionary #TODO.

    Returns
    -------
        The modified string.

    """
    for name in names:
        for result in precompiled[name].findall(string):
            string = string.replace(result, name)
    return string


class SimaProCSVExtractor(object):
    """
    Extract datasets from SimaPro CSV export files.

    The CSV file should be in a specific format, with row 1 containing either the string "SimaPro" or "CSV separator."

    Parameters
    ----------
    filepath : str
        The path to the SimaPro CSV export file.
    delimiter : str, optional
        The delimiter in the CSV file. Default is ";".
    name : str, optional
        The name of the project. If the name is not provided, it is extracted from the CSV file.
    encoding: str, optional
        The character encoding in the SimaPro CSV file. Defaults to "cp1252".

    Returns
    -------
    datasets : list
        The list of extracted datasets from the CSV file.
    global_parameters : dict
        The dictionary of global parameters for the CSV file.
    project_metadata : dict
        The dictionary of project metadata.

    Raises
    ------
    AssertionError:
        If the CSV file is not a valid Simapro export file.

    """

    @classmethod
    def extract(cls, filepath, delimiter=";", name=None, encoding="cp1252"):
        """
        Extract data from a SimaPro export file (.csv) and returns a list of datasets, global parameters, and project metadata.

        Parameters:
        -----------
        filepath : str
            The file path of the SimaPro export file to extract data from.
        delimiter : str, optional
            The delimiter used in the SimaPro export file. Defaults to ";".
        name : str, optional
            The name of the project. If not provided, the method will attempt to infer it from the SimaPro export file.
        encoding : str, optional
            The character encoding of the SimaPro export file. Defaults to "cp1252".

        Returns:
        --------
        Tuple[List[Dict], Dict, Dict]
            A tuple containing:
                - a list of dictionaries representing each dataset extracted from the SimaPro export file,
                - a dictionary containing global parameters extracted from the SimaPro export file, and
                - a dictionary containing project metadata extracted from the SimaPro export file.
        """
        assert os.path.exists(filepath), "Can't find file %s" % filepath
        log, logfile = get_io_logger("SimaPro-extractor")

        log.info(
            INTRODUCTION
            % (
                filepath,
                repr(delimiter),
                name,
            )
        )
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
        global_parameters, global_precompiled = cls.get_global_parameters(
            lines, project_metadata
        )

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
                    global_precompiled,
                )
                datasets.append(ds)
                index = cls.get_next_process_index(lines, index)
            except EndOfDatasets:
                break

        close_log(log)
        return datasets, global_parameters, project_metadata

    @classmethod
    def get_next_process_index(cls, data, index):
        """
        Get the index of the next process in the given data.

        Parameters:
        -----------
        data : List[List[str]]
            The data to search for the next process.
        index : int
            The index to start the search from.

        Returns:
        --------
        int
            The index of the next process in the data.

        """
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
        """
        Parse metadata from a list of strings and returns a dictionary of metadata key-value pairs.

        Parameters
        ----------
        data : list
            A list of strings containing metadata in the format "{key}: {value}".

        Returns
        -------
        dict
            A dictionary of metadata key-value pairs extracted from the input `data` list.

        Raises
        ------
        ValueError
            If a line of metadata does not contain a colon `:` character, or if it contains multiple colons.
        AssertionError
            If a line of metadata does not start and end with curly braces `{}`.

        Notes
        -----
        This method assumes that each line in the input `data` list contains only one metadata key-value pair,
        and that the key and value are separated by a single colon `:` character.

        Examples
        --------
        >>> data = ["{name}: John Smith", "{age}: 25", "", "{country: UK}"]
        >>> meta = get_project_metadata(data)
        >>> print(meta)
        {"name": "John Smith", "age": "25", "country": "UK"}

        """
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
        """
        Extract and return global parameters from a SimaPro export file.

        Args:
            data (List[List[str]]): A list of lists containing the data read from the SimaPro export file.
            pm (Dict[str, str]): A dictionary containing project metadata extracted from the SimaPro export file.

        Returns:
            A tuple containing:
                - parameters (Dict[str, Dict[str, Any]]): A dictionary containing global parameters extracted from the SimaPro export file. Each parameter is represented as a dictionary with keys 'name', 'unit', 'formula', and 'amount'.
                - global_precompiled (Dict[str, Pattern]): A dictionary containing compiled regular expression patterns used to search for parameter names in the SimaPro export file.

        Raises:
            ValueError: If an invalid parameter is encountered in the SimaPro export file.

        """
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

        # Extract name and uppercase
        parameters = {obj.pop("name").upper(): obj for obj in parameters}
        global_precompiled = {
            name: re.compile(uppercase_expression.format(name), flags=re.IGNORECASE)
            for name in parameters
        }

        # Change all formula values to uppercase if referencing global parameters
        for obj in parameters.values():
            if "formula" in obj:
                obj["formula"] = replace_with_uppercase(
                    obj["formula"], parameters, global_precompiled
                )

        ParameterSet(parameters).evaluate_and_set_amount_field()
        return parameters, global_precompiled

    @classmethod
    def get_project_name(cls, data):
        """
        Extract the project name from the given data.

        Parameters
        ----------
        data : list
            A list of data, where each item is a list of strings representing a row of the data.

        Returns
        -------
        str
            The project name.

        Notes
        -----
        This method searches for a row in the data where the first item starts with "{Project:" or "{Projet:".
        If such a row is found, the project name is extracted from that row and returned. Otherwise, `None` is returned.

        """
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
        """
        Determine if the uncertainty data is invalid.

        Parameters
        ----------
        amount : str
            The amount of uncertainty.
        kind : str
            The kind of uncertainty.
        field1 : str
            The first field of uncertainty data.
        field2 : str
            The second field of uncertainty data.
        field3 : str
            The third field of uncertainty data.

        Returns
        -------
        bool
            `True` if the uncertainty data is invalid, `False` otherwise.

        Notes
        -----
        This method checks if the given uncertainty data is invalid based on the kind of uncertainty.
        If the kind is "Lognormal" and `amount` is empty or `field1` is "0" or "1", the uncertainty data is considered invalid.

        """
        if kind == "Lognormal" and (not amount or field1 == "0" or field1 == "1"):
            return True

    @classmethod
    def create_distribution(cls, amount, kind, field1, field2, field3):
        """
        Create a distribution based on the given uncertainty data.

        Parameters
        ----------
        amount : str
            The amount of uncertainty.
        kind : str
            The kind of uncertainty.
        field1 : str
            The first field of uncertainty data.
        field2 : str
            The second field of uncertainty data.
        field3 : str
            The third field of uncertainty data.

        Returns
        -------
        dict
            A dictionary representing the distribution.

        Raises
        ------
        ValueError
            If the given uncertainty type is unknown.

        Notes
        -----
        This method creates a distribution based on the given uncertainty data.
        The distribution is returned as a dictionary with the following keys:
        - "uncertainty type": the ID of the uncertainty type
        - "loc": the location parameter of the distribution
        - "amount": the amount of uncertainty
        Depending on the kind of uncertainty, other keys may be included:
        - "scale": the scale parameter of the distribution (for "Lognormal" and "Normal" uncertainties)
        - "minimum": the minimum value of the distribution (for "Triangle" and "Uniform" uncertainties)
        - "maximum": the maximum value of the distribution (for "Triangle" and "Uniform" uncertainties)
        - "negative": `True` if the amount of uncertainty is negative, `False` otherwise.
        If the kind of uncertainty is "Undefined", an undefined uncertainty distribution is created.
        If the kind of uncertainty is "Lognormal", a lognormal uncertainty distribution is created.
        If the kind of uncertainty is "Normal", a normal uncertainty distribution is created.
        If the kind of uncertainty is "Triangle", a triangular uncertainty distribution is created.
        If the kind of uncertainty is "Uniform", a uniform uncertainty distribution is created.
        If the kind of uncertainty is unknown, a ValueError is raised.

        """
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
        """
        Parse a line in the 'Calculated parameters' section of a SimaPro file and return a dictionary of its components.

        Parameters
        ----------
        line : List[str]
            The line to be parsed, with the first string being the name, the second string the formula, and
            subsequent strings comments associated with the parameter.
        pm : Dict[str, float]
            A dictionary mapping variable names to their values in the context of the parameter.

        Returns
        -------
        parsed_parameter : Dict[str, Union[str, List[str]]]
        A dictionary with the following keys:
        - 'name' : str
            The name of the parameter.
        - 'formula' : str
            The formula used in the parameter, with variables replaced by their values according to `pm`.
        - 'comment' : List[str]
            A list of comments on the parameter.
        Examples
        --------
        #TODO

        """
        return {
            "name": line[0],
            "formula": normalize_simapro_formulae(line[1], pm),
            "comment": "; ".join([x for x in line[2:] if x]),
        }

    @classmethod
    def parse_input_parameter(cls, line):
        """
        Parse input parameters section of a SimaPro file.

        0. name
        1. value (not formula)
        2. uncertainty type
        3. uncert. param.
        4. uncert. param.
        5. uncert. param.
        6. hidden ("Yes" or "No" - we ignore)
        7. comment

        Returns
        -------
        #TODO
        Examples
        --------
        #TODO

        """
        ds = cls.create_distribution(*line[1:6])
        ds.update({"name": line[0], "comment": "; ".join([x for x in line[7:] if x])})
        return ds

    @classmethod
    def parse_biosphere_flow(cls, line, category, pm):
        """
        Parse biosphere flow line.

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
        """
        Read metadata from a SIMAPRO dataset.

        Returns:
            Tuple[Dict[str, str], int]: A tuple containing the metadata as a dictionary and the index of the next line
            after the metadata.

        Raises:
            IndexError: If the index is out of range for the given dataset.
        """

        metadata = {}
        while True:
            if not data[index]:
                pass
            elif data[index] and data[index][0] in SIMAPRO_PRODUCTS:
                return metadata, index
            elif data[index] and data[index + 1] and data[index][0]:
                if not data[index + 2]:
                    metadata[data[index][0]] = data[index + 1][0]
                    index += 1
                else:
                    # Scanning the following lines until a blank one is found to add all the non-empty following lines
                    # to the metadata
                    metadata_key = data[index][0]
                    metadata_values = []
                    index += 1
                    while data[index] and data[index][0]:
                        metadata_values.append(data[index][0])
                        index += 1
                    metadata[metadata_key] = metadata_values

            index += 1

    @classmethod
    def read_data_set(cls, data, index, db_name, filepath, gp, pm, global_precompiled):
        metadata, index = cls.read_dataset_metadata(data, index)
        """
        Read a SimaPro data set from a list of tuples.

        Returns
        -------
        Tuple[Dict[str, Any], int]
            A dictionary representing the SimaPro data set and the index where the reading stopped.

        Raises
        ------
        EndOfDatasets
            If the end of the SimaPro data set is reached.
        
        """
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

        # Extract name and uppercase
        ds["parameters"] = {obj.pop("name").upper(): obj for obj in ds["parameters"]}
        local_precompiled = {
            name: re.compile(uppercase_expression.format(name), flags=re.IGNORECASE)
            for name in ds["parameters"]
        }

        # Change all parameter formula values to uppercase if referencing
        # global or local parameters
        for obj in ds["parameters"].values():
            if "formula" in obj:
                obj["formula"] = replace_with_uppercase(
                    obj["formula"], ds["parameters"], local_precompiled
                )
                obj["formula"] = replace_with_uppercase(
                    obj["formula"], gp, global_precompiled
                )
        # Change all exchange values to uppercase if referencing
        # global or local parameters
        for obj in ds["exchanges"]:
            if "formula" in obj:
                obj["formula"] = replace_with_uppercase(
                    obj["formula"], ds["parameters"], local_precompiled
                )
                obj["formula"] = replace_with_uppercase(
                    obj["formula"], gp, global_precompiled
                )

        ps = ParameterSet(
            ds["parameters"], {key: value["amount"] for key, value in gp.items()}
        )
        # Changes in-place
        ps(ds["exchanges"])

        if not ds["parameters"]:
            del ds["parameters"]

        return ds, index
