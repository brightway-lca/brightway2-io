import csv
import os


class CSVExtractor(object):
    """
    Extracts data from CSV files.

    See Also:
    ---------
    - :class:`.ExcelExtractor`: Extracts data from Excel files.

    References:
    -----------
    - https://docs.python.org/3/library/csv.html: Official documentation for the csv module in Python.

    """

    @classmethod
    def extract(cls, filepath, encoding="utf-8-sig"):
        """
        Extracts CSV file data from the filepath.

        Parameters:
        ----------
        filepath : str
            The path to the CSV file.
        encoding : str, optional
            The encoding of the CSV file, with default being "utf-8-sig".

        Returns:
        -------
        list
            A list containing the filename and the contents of the CSV file.

        Raises:
        ------
        AssertionError
            If the file does not exist.

        Examples:
        --------
        >>> CSVExtractor.extract("example.csv")
        ["example.csv", [["1", "2", "3"], ["4", "5", "6"]]]
        """
        assert os.path.exists(filepath), "Can't file file at path {}".format(filepath)
        with open(filepath, encoding=encoding) as f:
            reader = csv.reader(f)
            data = [row for row in reader]
        return [os.path.basename(filepath), data]