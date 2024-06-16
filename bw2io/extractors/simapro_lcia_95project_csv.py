import csv
from pathlib import Path

from bw2data.logs import close_log, get_io_logger
from stats_arrays import *

from bw2io.utils import standardize_method_to_len_3

# SKIPPABLE_SECTIONS = {
#     "Airborne emissions",
#     "Economic issues",
#     "Emissions to soil",
#     "Final waste flows",
#     "Quantities",
#     "Raw materials",
#     "Units",
#     "Waterborne emissions",
# }


class EndOfDatasets(Exception):
    pass


class SimaProLCIA95ProjectCSVExtractor:
    """
    Extract data from SimaPro LCIA 9.5 Project CSV file format.

    Differs from `SimaProLCIACSVExtractor` in that this format seems not to use
    `End` at the end of sections.

    Parameters
    ----------
    filepath: str
        Filepath of the SimaPro LCIACSV file.
    delimiter: str, optional (default: ";")
        Delimiter used in the SimaPro LCIACSV file.
    encoding: str, optional (default: "cp1252")
        Encoding of the SimaPro LCIACSV file.

    Raises
    ------
    AssertionError
        If the filepath does not exist or the file is not a valid SimaPro
        export file.

    Returns
    -------
    list
        List of impact categories extracted from the SimaPro file.
    """

    @classmethod
    def extract(cls, filepath: Path, delimiter: str = ";", encoding: str = "cp1252"):
        filepath = Path(filepath)
        assert filepath.is_file(), f"Can't find file {filepath}"
        log, logfile = get_io_logger("SimaPro-LCIA-extractor")

        log.info(
            f"""Starting SimaPro import:
    Filepath: {filepath}
    Delimiter: {delimiter}"""
        )

        strip_delete = lambda obj: (
            obj.strip().replace("\x7f", "") if isinstance(obj, str) else obj
        )
        empty_lines = lambda line: line if any(line) else None

        with open(filepath, "r", encoding=encoding) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            lines = [[strip_delete(elem) for elem in line] for line in reader]

        # Check if valid SimaPro file
        assert "SimaPro" in lines[0][0], "File is not valid SimaPro export"

        impact_categories, context = [], {}
        sections = cls.clean_sections(cls.split_into_sections(lines))

        for section in sections:
            if section[0][0].startswith("SimaPro"):
                context["simapro version"] = section[0][1]
            elif section[0][0] == "Name":
                context["method"] = section[0][1]
            elif section[0][0] == "Comment":
                context["comment"] = "\n".join([line[1] for line in section])
            elif section[0][0].startswith("Use"):
                context["configuration"] = dict(section)
            elif section[0][0] == "Impact category":
                impact_categories.append(
                    {
                        "impact category": section[0][1],
                        "unit": section[0][2],
                        "cfs": [cls.parse_cf(line) for line in section[1:]],
                        **context,
                    }
                )
            elif section[0][0] == "Normalization-Weighting set":
                continue
            elif section[0][0] == "Normalization":
                pass
            elif section[0][0] == "Weighting":
                pass

        close_log(log)
        return impact_categories

    @classmethod
    def clean_sections(cls, sections: list) -> list:
        """Remove empty sections, and empty lines from sections"""
        return [
            [line for line in section if line != []]
            for section in sections
            if section != [[]]
        ]

    @classmethod
    def split_into_sections(cls, data: list) -> list:
        """Split the SimaPro file into sections using the blank line pattern"""
        split_locations = [2]

        for index, line in enumerate(data):
            if line == []:
                split_locations.append(index + 1)

        sections = (
            [data[: split_locations[0]]]
            + [
                data[split_locations[index] : split_locations[index + 1]]
                for index in range(len(split_locations) - 1)
            ]
            + [data[split_locations[-1] :]]
        )

        return sections

    @classmethod
    def parse_cf(cls, line):
        """Parse line in `Substances` section.

        0. category
        1. subcategory
        2. flow
        3. CAS number
        4. CF
        5. unit
        6. damage rate

        """
        return {
            "categories": (line[0], line[1]),
            "name": line[2],
            "CAS number": line[3],
            "amount": float(line[4].replace(",", ".")),
            "unit": line[5],
            "damage_rate": line[6] if len(line) >= 7 else None,
        }

    # @classmethod
    # def read_method_data_set(cls, data, index, filepath):
    #     """
    #     Read method data set from `data` starting at `index`.

    #     Parameters
    #     ----------
    #     data : list
    #         A list of lists containing the data to be processed.
    #     index : int
    #         The starting index to read method data set from.
    #     filepath : str
    #         The file path of the method data set.

    #     Returns
    #     -------
    #     list
    #         A list of completed method data sets.
    #     int
    #         The index where the method data set reading ended.

    #     Raises
    #     ------
    #     ValueError

    #     """
    #     metadata, index = cls.read_metadata(data, index)
    #     method_root_name = metadata.pop("Name")
    #     description = metadata.pop("Comment")
    #     category_data, nw_data, damage_category_data, completed_data = [], [], [], []

    #     # `index` is now the `Impact category` line
    #     while not data[index] or data[index][0] != "End":
    #         if not data[index] or not data[index][0]:
    #             index += 1
    #         elif data[index][0] == "Impact category":
    #             catdata, index = cls.get_category_data(data, index + 1)
    #             category_data.append(catdata)
    #         elif data[index][0] == "Normalization-Weighting set":
    #             nw_dataset, index = cls.get_normalization_weighting_data(
    #                 data, index + 1
    #             )
    #             nw_data.append(nw_dataset)
    #         elif data[index][0] == "Damage category":
    #             catdata, index = cls.get_damage_category_data(data, index + 1)
    #             damage_category_data.append(catdata)
    #         else:
    #             raise ValueError

    #     for ds in category_data:
    #         completed_data.append(
    #             {
    #                 "description": description,
    #                 "name": (method_root_name, ds[0]),
    #                 "unit": ds[1],
    #                 "filename": filepath,
    #                 "exchanges": ds[2],
    #             }
    #         )

    #     for ds in nw_data:
    #         completed_data.append(
    #             {
    #                 "description": description,
    #                 "name": (method_root_name, ds[0]),
    #                 "unit": metadata["Weighting unit"],
    #                 "filename": filepath,
    #                 "exchanges": cls.get_all_cfs(ds[1], category_data),
    #             }
    #         )

    #     for ds in damage_category_data:
    #         completed_data.append(
    #             {
    #                 "description": description,
    #                 "name": (method_root_name, ds[0]),
    #                 "unit": ds[1],
    #                 "filename": filepath,
    #                 "exchanges": cls.get_damage_exchanges(ds[2], category_data),
    #             }
    #         )

    #     return completed_data, index

    # @classmethod
    # def get_all_cfs(cls, nw_data, category_data):
    #     """
    #     Get all CFs from `nw_data` and `category_data`.

    #     Parameters
    #     ----------
    #     nw_data : list
    #         A list of tuples containing normalization-weighting (NW) set names and scales.
    #     category_data : list
    #         A list of tuples containing impact category names, units, and CF data.
    #     Returns
    #     -------
    #     list
    #         A list of all CFs.
    #     """

    #     def rescale(cf, scale):
    #         cf["amount"] *= scale
    #         return cf

    #     cfs = []
    #     for nw_name, scale in nw_data:
    #         for cat_name, _, cf_data in category_data:
    #             if cat_name == nw_name:
    #                 cfs.extend([rescale(cf, scale) for cf in cf_data])
    #     return cfs

    # @classmethod
    # def get_damage_exchanges(cls, damage_data, category_data):
    #     """
    #     Calculate the damage exchanges based on damage data and category data.

    #     Parameters
    #     ----------
    #     damage_data : list of tuples
    #         A list of tuples containing the name and scale of the damage
    #     category_data : list of tuples
    #         A list of tuples containing the name, unit, and data of each impact category

    #     Returns
    #     -------
    #     list of dictionaries
    #         A list of dictionaries with the calculated damage exchanges of each impact category
    #     """

    #     def rescale(cf, scale):
    #         cf["amount"] *= scale
    #         return cf

    #     cfs = []
    #     for damage_name, scale in damage_data:
    #         for cat_name, _, cf_data in category_data:
    #             if cat_name == damage_name:
    #                 # Multiple impact categories might use the same exchanges
    #                 # So scale and increment the amount if it exists, scale and append if it doesn't
    #                 for cf in cf_data:
    #                     c_name, c_categories = cf["name"], cf["categories"]
    #                     found_cf = False
    #                     for existing_cf in cfs:
    #                         if (
    #                             existing_cf["name"] == c_name
    #                             and existing_cf["categories"] == c_categories
    #                         ):
    #                             existing_cf["amount"] += cf["amount"] * scale
    #                             found_cf = True
    #                             continue
    #                 if found_cf:
    #                     continue
    #                 cfs.extend([rescale(cf, scale) for cf in cf_data])
    #     return cfs

    # @classmethod
    # def get_category_data(cls, data, index):
    #     """
    #     Parse impact category data and return its name, unit, and data.

    #     Parameters
    #     ----------
    #     data : list of lists
    #         A list of lists with the data for all categories
    #     index : int
    #         The index of the current impact category in the list

    #     Returns
    #     -------
    #     tuple
    #         A tuple with the name, unit, and data for the impact category
    #     """
    #     cf_data = []
    #     # First line is name and unit
    #     name, unit = data[index][:2]
    #     index += 2
    #     assert data[index][0] == "Substances"
    #     index += 1
    #     while data[index]:
    #         cf_data.append(cls.parse_cf(data[index]))
    #         index += 1
    #     return (name, unit, cf_data), index

    # @classmethod
    # def get_damage_category_data(cls, data, index):
    #     """
    #     Parse damage category data and return the name, unit, and data of the category.

    #     Parameters
    #     ----------
    #     data : list of lists
    #         A list of lists with the data of the damage categories
    #     index : int
    #         The index of the current damage category in the list

    #     Returns
    #     -------
    #     tuple
    #         A tuple with the name, unit, and data for the damage category
    #     """
    #     damage_data = []
    #     # First line is name and unit
    #     name, unit = data[index][:2]
    #     index += 2
    #     assert data[index][0] == "Impact categories"
    #     index += 1
    #     while data[index]:
    #         method, scalar = data[index][:2]
    #         damage_data.append((method, float(scalar.replace(",", "."))))
    #         index += 1
    #     return (name, unit, damage_data), index

    # @classmethod
    # def get_normalization_weighting_data(cls, data, index):
    #     # TODO: Only works for weighting data, no addition or normalization
    #     nw_data = []
    #     name = data[index][0]
    #     index += 2
    #     assert data[index][0] == "Weighting"
    #     index += 1
    #     while data[index]:
    #         cat, weight = data[index][:2]
    #         index += 1
    #         if weight == "0":
    #             continue
    #         nw_data.append((cat, float(weight.replace(",", "."))))
    #     return (name, nw_data), index
