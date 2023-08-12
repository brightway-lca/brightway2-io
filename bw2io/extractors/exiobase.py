import csv
import re
from pathlib import Path
import zipfile

from tqdm import tqdm


def remove_numerics(string):
    """
    Remove numeric values enclosed in parentheses from a given string, e.g. 'Tobacco products (16)' into 'Tobacco products'. 

    Parameters
    ----------
    string : str
        The string to be processed

    Returns
    -------
    str
        The processed string without numeric values enclosed in parentheses.
    """
    return re.sub(r" \(\d\d\)$", "", string)



class Exiobase3MonetaryDataExtractor(object):
    @classmethod
    def _get_path(cls, dirpath):
        """
        Get the directory path of the EXIOBASE data file.

        Parameters
        ----------
        dirpath : str
            The path of the EXIOBASE data file or directory.

        Returns
        -------
        Path
            The directory path of the EXIOBASE data file.
        """
        path = Path(dirpath)
        if path.is_file() and path.suffix.lower() == ".zip":
            zf = zipfile.ZipFile(path)
            if zf.namelist()[0].startswith("IOT_"):
                root_dir = zf.namelist()[0].split("/")[0]
                path = zipfile.Path(zf, root_dir)
            else:
                path = zipfile.Path(zf)
        else:
            assert path.is_dir(), "Must supply path to EXIOBASE data folder"
            assert (path / "A.txt").is_file(), "Directory path must include Exiobase files"
        return path

    @classmethod
    def _get_production_volumes(cls, dirpath):
        """
        Extract production volumes from the EXIOBASE data file.

        Parameters
        ----------
        dirpath : str
            The path of the EXIOBASE data file or directory.

        Returns
        -------
        dict
            A dictionary for the production volume.
        """
        if not (dirpath / "x.txt").is_file():
            return {}
        with (dirpath / "x.txt").open() as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            data = {
                (row["sector"], row["region"]): float(row["indout"]) for row in reader
            }
        return data

    @classmethod
    def _get_unit_data(cls, dirpath):
        """
        Extract unit data from the EXIOBASE data file.

        Parameters
        ----------
        dirpath : str
            The path to the EXIOBASE data file or directory.

        Returns
        -------
        dict
            A dctionary of unit data from the EXIOBase data file.
        """
        lookup = {"M.EUR": "million €"}

        with (dirpath / "unit.txt").open() as csvfile:
            reader = csv.DictReader(csvfile, delimiter="\t")
            data = {
                (row["sector"], row["region"]): lookup[row["unit"]] for row in reader
            }
        return data

    @classmethod
    def get_flows(cls, dirpath):
        """
        Extract flows from an EXIOBASE data file.

        Parameters
        ----------
        dirpath : str
            The path of the EXIOBASE data file or directory.

        Returns
        -------
        dict
            A dictionary of flows from the EXIOBASE data file.
        """
        dirpath = cls._get_path(dirpath)

        with (dirpath / "satellite" / "unit.txt").open() as csvfile:
            reader = csv.reader(csvfile, delimiter="\t")
            next(reader)
            data = {o[0]: o[1] for o in reader}
        return data

    @classmethod
    def get_products(cls, dirpath):
        """
        Get product information from a given directory.

        Parameters
        ----------
        dirpath : str
            The path to the directory with the product information.

        Returns
        -------
        list
            A list of dictionaries with the following keys:
            - 'name': str
            The product name.
            - 'location': str
            The product location.
            - 'unit': str
            The product's unit of measure.
            - 'production volume': float
            The total production volume for the product.
        """
        dirpath = cls._get_path(dirpath)

        units = cls._get_unit_data(dirpath)
        volumes = cls._get_production_volumes(dirpath)
        return [
            {
                "name": key[0],
                "location": key[1],
                "unit": units[key],
                "production volume": volumes.get(key, 0),
            }
            for key in units
        ]

    @classmethod
    def get_technosphere_iterator(
        cls, dirpath, num_products, ignore_small_balancing_corrections=True
    ):
        """
        Get an iterator in a given directory.

        Parameters
        ----------
        dirpath : str
            The path to the directory with the data.
        num_products : int
            The number of products.
        ignore_small_balancing_corrections : bool, optional
            Ignore small balancing corrections. By default True.    
        """
        dirpath = cls._get_path(dirpath)

        with (dirpath / "A.txt").open() as f:
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
        """
        Returns an iterator that yields tuples of flow names, locations, and amounts.

        Parameters
        ----------
        dirpath : str
            The path to the directory.
        ignore_small_balancing_corrections : bool, optional
            Ignore small balancing corrections. By default True.
        """
        dirpath = cls._get_path(dirpath)
        with (dirpath / "satellite" / "S.txt").open() as f:
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
