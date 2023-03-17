import os

from openpyxl import load_workbook


def get_cell_value_handle_error(cell):
    """
    Get the value of a cell, handling error types.

    Parameters
    ----------
    cell : openpyxl.cell.cell.cell
        The cell to retrieve the value from.

    Returns
    -------
    object
        The value of the cell, or None if the cell has an error type.
    """
    if cell.data_type == "e":
        # Error type
        return None
    else:
        return cell.value


class ExcelExtractor(object):
    """
    Extract data from an Excel file.

    Parameters
    ----------
    object : type
        The parent object for the ExcelExtractor class

    Returns
    -------
    object
        An instance of the class
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
        Extract data from a single sheet in an Excel file.

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
            A list of lists containing the data from each cell in the sheet.
        """
        ws = wb[name]
        _ = lambda x: x.strip() if (strip and hasattr(x, "strip")) else x
        return [
            [_(get_cell_value_handle_error(cell)) for cell in row] for row in ws.rows
        ]
