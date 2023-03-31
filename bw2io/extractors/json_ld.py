import json
from pathlib import Path

FILES_TO_IGNORE = {
    "context.json",
    "layout.json",
}

DIRECTORIES_TO_IGNORE = {
    "bin",
}


class JSONLDExtractor(object):
    """Extract JSON-LD from a directory.

    Attributes
    ----------
    FILES_TO_IGNORE: set
        Files to ignore when extracting JSON-LD data.
    DIRECTORIES_TO_IGNORE: set
        Directories to ignore when extracting JSON-LD data.
    """
    @classmethod
    def extract(cls, filepath, add_filename=True):
        """
        Extracts JSON-LD data from the filepath.

        Parameters
        ----------
        filepath : str or Path
            The path of the directory from which data will be extracted
        add_filename : bool, optional
            Add the name to the extracted data. By default, True.
        
        Returns
        -------
        dict
            A dictionary with the extracted JSON-LD data.
        
        Raises
        ------
        ValueError
            If the file is not a zip archive.
        NotImplementedError
            If extraction of zip archives is not yet supported.
        """
        def adder(data, filepath, add_filename):
            """
            Adds the filename to the extracted data.

            Parameters
            ----------
            data : dict
                The extracted data.
            filepath : Path
                The path of the file from which the data was extracted.
            add_filename : bool
                Add the filename to the extracted data.

            Returns
            -------
            dict
                The extracted data with the filename added (if add_filename = True).
            """
            if not add_filename:
                    return data
            else:
                data["filename"] = str(filepath)
                return data

        filepath = Path(filepath)
        if filepath.is_file():
            if not filepath.suffix == ".zip":
                raise ValueError(
                    "File not supported:\n\t`%s` is a file but not a zip archive."
                )
            else:
                raise NotImplementedError(
                    "Extraction of zip archives not yet supported"
                )
        else:
            assert filepath.is_dir()
            filepath = filepath.resolve()

            # Assume directory is one level deep
            data = {
                directory.name: dict(
                    sorted(
                        [
                            (
                                fp.stem,
                                adder(
                                    json.load(open(fp, encoding="utf-8")),
                                    fp,
                                    add_filename,
                                ),
                            )
                            for fp in directory.iterdir()
                            if fp.name not in FILES_TO_IGNORE
                            and not fp.name.startswith(".")
                            and "json" in fp.suffix.lower()
                        ]
                    )
                )
                for directory in filepath.iterdir()
                if directory.is_dir() and directory.name not in DIRECTORIES_TO_IGNORE
            }

        return data
