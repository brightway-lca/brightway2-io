from pathlib import Path
from tqdm import tqdm
import csv
import os
import re


def remove_numerics(string):
    """Transform names like 'Tobacco products (16)' into 'Tobacco products'"""
    return re.sub(r" \(\d\d\)$", "", string)


class Exiobase3MonetaryDataExtractor(object):
    @classmethod
    def _check_dir(cls, path):
        # Note: this assumes industry by industry
        assert os.path.isdir(path), "Must supply path to EXIOBASE data folder"
        assert "x.txt" in os.listdir(path), "Directory path must include Exiobase files"

    @classmethod
    def _get_production_volumes(cls, dirpath):
        with open(dirpath / "x.txt") as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            data = {
                (row["sector"], row["region"]): float(row["indout"]) for row in reader
            }
        return data

    @classmethod
    def _get_unit_data(cls, dirpath):
        lookup = {"M.EUR": "million €"}

        with open(dirpath / "unit.txt") as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            data = {
                (row["sector"], row["region"]): lookup[row["unit"]] for row in reader
            }
        return data

    @classmethod
    def get_flows(cls, dirpath):
        dirpath = Path(dirpath)

        with open(dirpath / "satellite" / "unit.txt") as csvfile:
            reader = csv.reader(csvfile, delimiter="\t")
            next(reader)
            data = {o[0]: o[1] for o in reader}
        return data

    @classmethod
    def get_products(cls, dirpath):
        dirpath = Path(dirpath)

        cls._check_dir(dirpath)
        units = cls._get_unit_data(dirpath)
        volumes = cls._get_production_volumes(dirpath)
        return [
            {
                "name": key[0],
                "location": key[1],
                "unit": units[key],
                "production volume": volumes[key],
            }
            for key in units
        ]

    @classmethod
    def get_technosphere_iterator(
        cls, dirpath, num_products, ignore_small_balancing_corrections=True
    ):
        dirpath = Path(dirpath)

        with open(dirpath / "A.txt") as f:
            reader = csv.reader(f, delimiter="\t")
            locations = next(reader)[2:]
            names = [remove_numerics(o) for o in next(reader)[2:]]

            for line in tqdm(reader):
                inpt = (remove_numerics(line[1]), line[0])
                for index, elem in enumerate(line[2:]):
                    if elem and float(elem) != 0:
                        if (
                            ignore_small_balancing_corrections
                            and abs(float(elem)) < 1e-15
                        ):
                            continue
                        else:
                            yield (inpt, (names[index], locations[index]), float(elem))

    @classmethod
    def get_biosphere_iterator(cls, dirpath, ignore_small_balancing_corrections=True):
        dirpath = Path(dirpath)

        with open(dirpath / "satellite" / "S.txt") as f:
            reader = csv.reader(f, delimiter="\t")
            locations = next(reader)[1:]
            names = [remove_numerics(o) for o in next(reader)[1:]]

            for line in tqdm(reader):
                flow = line[0]
                for index, elem in enumerate(line[1:]):
                    if elem and float(elem) != 0:
                        if (
                            ignore_small_balancing_corrections
                            and abs(float(elem)) < 1e-15
                        ):
                            continue
                        else:
                            yield (flow, (names[index], locations[index]), float(elem))
