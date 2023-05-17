import os

from openpyxl import load_workbook


def get_cell_value_handle_error(cell):
    """
    Retrieve the value of a given cell and handle error types.

    Parameters
    ----------
    cell : openpyxl.cell.cell.Cell
        The cell to get the value from.

    Returns
    --------
    object
        The value of the cell, or None if the cell has an error type.

    Examples
    ---------
    >>> from openpyxl import Workbook
    >>> wb = Workbook()
    >>> ws = wb.active
    >>> ws["A1"] = "hello"
    >>> assert get_cell_value_handle_error(ws["A1"]) == "hello"
    >>> ws["B1"] = "=1/0"
    >>> assert get_cell_value_handle_error(ws["B1"]) == None
    """
    if cell.data_type == "e":
        # Error type
        return None
    else:
        return cell.value


class ExcelExtractor(object):
    """
    A class used to extract data from an Excel file.
    
    Parameters
    ----------
    object : type
        The parent object for the ExcelExtractor class.

    Returns
    -------
    object
        An instance of the class.

    See Also
    --------
    openpyxl.load_workbook : Load a workbook from a file.

    Notes
    -----
    This class requires the openpyxl package to be installed.

    Raises
    ------
    AssertionError
        If the file at 'filepath' does not exist.

    Parameters
    ----------
    filepath : str
        The path to the Excel file.

    Returns
    -------
    list
        A list of tuples containing the name of each sheet in the file and the data from each sheet.

    Examples
    --------
    >>> extractor = ExcelExtractor()
    >>> filepath = 'example.xlsx'
    >>> data = extractor.extract(filepath)
    """
    @classmethod
    def extract(cls, filepath):
        """
        Extract data from an Excel file.

        Parameters
        ----------
        filepath : str
            The path to the Excel file.

        Returns
        -------
        list
            A list of tuples containing the name of each sheet in the file and the data from each sheet.

        Raises
        ------
        AssertionError
            If the file at 'filepath' does not exist.
        """
        assert os.path.exists(filepath), "Can't file file at path {}".format(filepath)
        wb = load_workbook(filepath, data_only=True, read_only=True)
        data = [(name, cls.extract_sheet(wb, name)) for name in wb.sheetnames]
        wb.close()
        return data

    @classmethod
    def extract_sheet(cls, wb, name, strip=True):
        """
        Extract data from a single sheet in an Excel workbook.

        Parameters
        ----------
        wb : openpyxl.workbook.Workbook
            The workbook object with the sheet to extract data from.
        name : str
            The name of the sheet to extract data from.
        strip : bool, optional
            If True, strip whitespace from cell values, by default True.

        Returns
        -------
        list
            A list of lists containing the data from the sheet.

        Notes
        -----
        This method is called by the 'extract' method to extract the data from each sheet in the workbook.

        Examples
        --------
        >>> wb = openpyxl.load_workbook('example.xlsx')
        >>> name = 'Sheet1'
        >>> data = ExcelExtractor.extract_sheet(wb, sheetname)
        """
        ws = wb[name]
        _ = lambda x: x.strip() if (strip and hasattr(x, "strip")) else x
        return [
            [_(get_cell_value_handle_error(cell)) for cell in row] for row in ws.rows
        ]
