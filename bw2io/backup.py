import codecs
import datetime
import json
import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, Union

from bw2data import projects
from bw_processing import safe_filename

_METADATA_FIELDS = {"is_sourced", "revision", "data", "full_hash"}


def _add_project_metadata() -> None:
    fp = projects.dir / "project-metadata.json"
    data = {
        field: getattr(projects.dataset, field)
        for field in _METADATA_FIELDS
        if getattr(projects.dataset, field)
    }
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _remove_project_metadata() -> None:
    fp = projects.dir / "project-metadata.json"
    if fp.is_file():
        fp.unlink()


def _restore_project_metadata() -> None:
    if not (projects.dir / "project-metadata.json").is_file():
        return
    metadata = json.load(open(projects.dir / "project-metadata.json", encoding="utf-8"))
    for field in _METADATA_FIELDS:
        if field in metadata:
            setattr(projects.dataset, field, metadata[field])
    projects.dataset.save()


def _extract_single_directory_tarball(filepath: Path, output_dir: Path) -> Path:
    def is_within_directory(directory, target):
        abs_directory = os.path.abspath(directory)
        abs_target = os.path.abspath(target)
        prefix = os.path.commonprefix([abs_directory, abs_target])
        return prefix == abs_directory

    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        for member in tar.getmembers():
            member_path = os.path.join(path, member.name)
            if not is_within_directory(path, member_path):
                raise Exception("Attempted path traversal in tar file")

        tar.extractall(path, members, numeric_owner=numeric_owner)

    with tarfile.open(filepath, "r:gz") as tar:
        safe_extract(tar, output_dir)

    # Find single extracted directory; don't know it ahead of time
    extracted_dirs = [
        (Path(output_dir) / dirname)
        for dirname in Path(output_dir).iterdir()
        if (Path(output_dir) / dirname).is_dir()
    ]
    if not len(extracted_dirs) == 1:
        raise ValueError("Can't find single directory extracted from project archive")
    return extracted_dirs[0]


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
    """

    dir_backup = Path(dir_backup or Path.home())

    # Check if the backup directory exists and is writable
    if not dir_backup.exists():
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


def backup_project_directory(
    project: str,
    timestamp: Optional[bool] = True,
    dir_backup: Optional[Union[str, Path]] = None,
) -> Path:
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
    filepath : Path
        pathlib.Path of archive file

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

    _add_project_metadata()
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

    _remove_project_metadata()
    return fp


def restore_project_directory(
    fp: Union[str, Path],
    project_name: Optional[str] = None,
    overwrite_existing: Optional[bool] = False,
    switch: bool = False,
):
    """
    Restore a backed up project data directory from a ``.tar.gz`` (compressed tar archive) specified by ``fp``. Choose a custom name, or use the name of the project in the archive. If the project already exists, you must set ``overwrite_existing`` to True.

    Parameters
    ----------
    fp : str, Path
        File path of the project to restore.
    project_name : str, optional
        Name of new project to create.
    overwrite_existing : bool, optional
    switch: bool, optional.
        Switch to new project after restoring it.

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
    fp_path = Path(fp)
    if not fp_path.is_file():
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
            f"Project {project_name} already exists, set `overwrite_existing=True` to overwrite"
        )

    with tempfile.TemporaryDirectory() as td:
        extracted_path = _extract_single_directory_tarball(filepath=fp, output_dir=td)

        _from_project_name = projects.current
        projects.set_current(project_name, update=False)
        shutil.copytree(extracted_path, projects.dir, dirs_exist_ok=True)

        _restore_project_metadata()
        _remove_project_metadata()

        if not switch:
            projects.set_current(_from_project_name)

        print(f"Restored project: {project_name}")

    if switch:
        projects.set_current(project_name, update=False)

    return project_name
