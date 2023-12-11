import codecs
import datetime
import json
import os
import sys
import shutil
import tarfile
import tempfile
from importlib import reload
from pathlib import Path
from typing import Optional, Union

from bw2data import projects
from bw_processing import safe_filename


def backup_data_directory(
    timestamp: Optional[bool] = True, dir_backup: Optional[Union[str, Path]] = None
):
    """
    Backup the Brightway2 data directory to a `.tar.gz` (compressed tar archive) in a specified directory, or in the user's home directory by default.

    The file name is of the form "brightway2-data-backup.{timestamp}.tar.gz", unless `timestamp` is False, in which case the file name is "brightway2-data-backup.tar.gz".

    Parameters
    ----------
    timestamp : bool, optional
        If True, append a timestamp to the backup file name.

    dir_backup : str, Path, optional
        Directory to backup. If None, use the user's home directory.

    Raises
    ------
    FileNotFoundError
        If the backup directory does not exist.
    PermissionError
        If the backup directory is not writable.

    Examples
    --------
    >>> import bw2io
    >>> bw2io.bw2setup()
    >>> bw2io.backup.backup_data_directory()
    Creating backup archive - this could take a few minutes...

    See Also
    --------
    bw2io.backup.restore_data_directory: To restore a data directory from a backup.
    """

    dir_backup = Path(dir_backup or Path.home())

    # Check if the backup directory exists and is writable
    if not dir_backup.is_dir():
        raise FileNotFoundError(f"The directory {dir_backup} does not exist.")
    if not os.access(dir_backup, os.W_OK):
        raise PermissionError(f"The directory {dir_backup} is not writable.")

    # Construct the backup file name
    timestamp_str = (
        f'.{datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p")}' if timestamp else ""
    )
    backup_filename = f"brightway2-data-backup{timestamp_str}.tar.gz"
    fp = dir_backup / backup_filename

    # Create the backup archive
    print(
        "Creating backup archive of data directory - this could take a few minutes..."
    )
    with tarfile.open(fp, "w:gz") as tar:
        data_directory = Path(projects._base_data_dir)
        tar.add(data_directory, arcname=data_directory.name)

    print(f"Saved to: {fp}")

    return fp


def restore_data_directory(
    fp: Union[str, Path],
    new_data_dir: Optional[Union[str, Path]] = None,
):
    """
    Restores a brightway data directory from a tar.gz file. If the data directory already exists, you must confirm that you want to delete it.
    If ``new_data_dir`` is specified, the data directory will be restored to that location. Otherwise, the data directory will be restored to the default location. e.g: ``~/.local/share/brightway3`` on Linux.

    Parameters
    ----------
    fp (Union[str, Path]):
        The file path of the tar.gz file.
    new_data_dir (Optional[Union[str, Path]]):
        The new data directory path.

    Raises
    ------
    ValueError:
        If the file doesn't exist
    PermissionError:
        If the data directory is not writable.
    Exception:
        If there's an attempted path traversal in the tar file.

    Returns
    -------
    data_dir (Path):
        The path of the restored data directory.

    See Also
    --------
    bw2io.backup.backup_data_directory: To backup the data directory.
    """

    # check if file exists
    fp = Path(fp)
    if not fp.is_file():
        raise ValueError(f"Can't find file at path: {fp}")

    # if new_data_dir, set the environment variable (this will not be persistent, I only know how to do that on Linux or in a venv...)
    if new_data_dir:
        os.environ["BRIGHTWAY2_DIR"] = str(new_data_dir)
        data_dir = Path(new_data_dir)
    else:
        data_dir = Path(projects._base_data_dir)

    # Confirm that the user wants to overwrite the data directory, if it exists
    if data_dir.is_dir():
        confirm_overwrite = input(
            f"This will overwrite your existing brightway data directory '{data_dir}'.\nAre you really, really sure that you want to do that? (y/n) "
        )
        if confirm_overwrite.lower() == "y":
            shutil.rmtree(data_dir)
        else:
            print("Aborting...")
            return

    # Extract the tar file
    print(
        "Restoring brightway data directory from backup archive - this could take a few minutes..."
    )
    with tempfile.TemporaryDirectory() as td:
        with tarfile.open(fp, "r:gz") as tar:

            def is_within_directory(directory, target):
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)

                prefix = os.path.commonprefix([abs_directory, abs_target])

                return prefix == abs_directory

            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")

                tar.extractall(path, members, numeric_owner=numeric_owner)

            safe_extract(tar, td)

        # Find single extracted directory; don't know it ahead of time
        extracted_dir = [
            (Path(td) / dirname)
            for dirname in Path(td).iterdir()
            if (Path(td) / dirname).is_dir()
        ]
        extracted_path = extracted_dir[0]
        shutil.copytree(extracted_path, data_dir)

    print(f"Restored brightway data to directory: {data_dir}")

    # this block is maybe totally wrong --> could be done better or removed
    if data_dir != Path(projects._base_data_dir):
        projects.change_base_directories(data_dir)
        print(
            f"""
        Environmental variable 'BRIGHTWAY2_DIR' now set to {projects._base_data_dir} 
        To use this data directory in future sessions, you must configure it.
        For example: 
        \t * in a python script, use `os.environ['BRIGHTWAY2_DIR'] = <path to data directory> before importing bw2data, or with `projects.change_base_directories(data_dir)`, after importing bw2data.
        \t * or in the terminal: `export BRIGHTWAY2_DIR=<path to data directory>` 
        \t  (add this to your venv activate script to make it persistent)
        """
        )

    return data_dir


def backup_project_directory(
    project: str,
    timestamp: Optional[bool] = True,
    dir_backup: Optional[Union[str, Path]] = None,
):
    """
    Backup project data directory to a ``.tar.gz`` (compressed tar archive) in the user's home directory, or a directory specified by ``dir_backup``.

    File name is of the form ``brightway2-project-{project}-backup{timestamp}.tar.gz``, unless ``timestamp`` is False, in which case the file name is ``brightway2-project-{project}-backup.tar.gz``.

    Parameters
    ----------
    project : str
        Name of the project to backup.

    timestamp : bool, optional
        If True, append a timestamp to the backup file name.

    dir_backup : str, Path, optional
        Directory to backup. If None, use the default (home)).

    Returns
    -------
    project_name : str
        Name of the project that was backed up.

    Raises
    ------
    ValueError
        If the project does not exist.
    FileNotFoundError
        If the backup directory does not exist.
    PermissionError
        If the backup directory is not writable.

    See Also
    --------
    bw2io.backup.restore_project_directory: To restore a project directory from a backup.
    """

    if project not in projects:
        raise ValueError("Project {} does not exist".format(project))

    dir_backup = Path(dir_backup or Path.home())

    # Check if the backup directory exists and is writable
    if not dir_backup.exists():
        raise FileNotFoundError(f"The directory {dir_backup} does not exist.")
    if not os.access(dir_backup, os.W_OK):
        raise PermissionError(f"The directory {dir_backup} is not writable.")

    timestamp_str = (
        datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p") if timestamp else ""
    )
    backup_filename = f"brightway2-project-{project}-backup{timestamp_str}.tar.gz"
    fp = dir_backup / backup_filename

    dir_path = Path(projects._base_data_dir) / safe_filename(project)

    (dir_path / ".project-name.json").write_text(json.dumps({"name": project}))

    print("Creating project backup archive - this could take a few minutes...")

    with tarfile.open(fp, "w:gz") as tar:
        tar.add(dir_path, arcname=safe_filename(project))

    print(f"Saved to: {fp}")

    return fp


def restore_project_directory(
    fp: Union[str, Path],
    project_name: Optional[str] = None,
    overwrite_existing: Optional[bool] = False,
):
    """
    Restore a backed up project data directory from a ``.tar.gz`` (compressed tar archive) specified by ``fp``. Choose a custom name, or use the name of the project in the archive. If the project already exists, you must set ``overwrite_existing`` to True.

    Parameters
    ----------
    fp : str, Path
        File path of the project to restore.
    project_name : str, optional
        Name of new project to create
    overwrite_existing : bool, optional

    Returns
    -------
    project_name : str, Path
        Name of the project that was restored.

    Raises
    ------
    FileNotFoundError
        If the file path does not exist.
    ValueError
        If the project name cannot be found in the archive.
        If the project exists and ``overwrite_existing`` is False.

    See Also
    --------
    bw2io.backup.backup_project_directory: To backup a project directory.
    """
    fp = Path(fp)
    if not fp.is_file():
        raise ValueError(f"Can't find file at path: {fp}")

    def get_project_name(fp):
        reader = codecs.getreader("utf-8")
        # See https://stackoverflow.com/questions/68997850/python-readlines-with-tar-file-gives-streamerror-seeking-backwards-is-not-al/68998071#68998071
        with tarfile.open(fp, "r:gz") as tar:
            for member in tar:
                if member.name[-17:] == "project-name.json":
                    return json.load(reader(tar.extractfile(member)))["name"]
            raise ValueError("Couldn't find project name file in archive")

    print("Restoring project backup archive - this could take a few minutes...")
    project_name = get_project_name(fp) if project_name is None else project_name

    if project_name in projects and not overwrite_existing:
        raise ValueError(
            f"Project {project_name} already exists, set overwrite_existing=True to overwrite"
        )

    with tempfile.TemporaryDirectory() as td:
        with tarfile.open(fp, "r:gz") as tar:

            def is_within_directory(directory, target):
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)

                prefix = os.path.commonprefix([abs_directory, abs_target])

                return prefix == abs_directory

            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")

                tar.extractall(path, members, numeric_owner=numeric_owner)

            safe_extract(tar, td)

        # Find single extracted directory; don't know it ahead of time
        extracted_dir = [
            (Path(td) / dirname)
            for dirname in Path(td).iterdir()
            if (Path(td) / dirname).is_dir()
        ]
        if not len(extracted_dir) == 1:
            raise ValueError(
                "Can't find single directory extracted from project archive"
            )
        extracted_path = extracted_dir[0]

        _current = projects.current
        projects.set_current(project_name, update=False)
        shutil.copytree(extracted_path, projects.dir, dirs_exist_ok=True)
        projects.set_current(_current)

        print(f"Restored project: {project_name}")

    return project_name
