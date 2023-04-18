from pathlib import Path
from typing import Optional
import codecs
import datetime
import json
import os
import shutil
import tarfile
import tempfile

from bw2data import projects
from bw_processing import safe_filename


def backup_data_directory():
    """
    Backup data directory to a ``.tar.gz`` (compressed tar archive) in the user's home directory.
    Restoration is done manually.

    Examples
    --------
    >>> bw2io.bw2setup()
    >>> bw2io.backup.backup_data_directory()
    Creating backup archive - this could take a few minutes...
    """
    fp = os.path.join(
        os.path.expanduser("~"),
        "brightway2-data-backup.{}.tar.gz".format(
            datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p")
        ),
    )
    print("Creating backup archive - this could take a few minutes...")
    with tarfile.open(fp, "w:gz") as tar:
        tar.add(projects.dir, arcname=os.path.basename(projects.dir))


def backup_project_directory(project: str):
    """
    Backup project data directory to a ``.tar.gz`` (compressed tar archive) in the user's home directory.

    Parameters
    ----------
    project : str
        Name of the project to backup.

    Returns
    -------
    project_name : str
        Name of the project that was backed up.

    Raises
    ------
    ValueError
       If the project does not exist.

    See Also
    --------
    bw2io.backup.restore_project_directory: To restore a project directory from a backup.
    """

    if project not in projects:
        raise ValueError("Project {} does not exist".format(project))

    fp = os.path.join(
        os.path.expanduser("~"),
        "brightway2-project-{}-backup.{}.tar.gz".format(
            project, datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p")
        ),
    )
    dir_path = os.path.join(projects._base_data_dir, safe_filename(project))
    with open(os.path.join(dir_path, ".project-name.json"), "w") as f:
        json.dump({"name": project}, f)
    print("Creating project backup archive - this could take a few minutes...")
    with tarfile.open(fp, "w:gz") as tar:
        tar.add(dir_path, arcname=safe_filename(project))

    return project


def restore_project_directory(fp: str, project_name: Optional[str] = None, overwrite_existing: Optional[bool] = False):
    """
    Restore a backed up project data directory from a ``.tar.gz`` (compressed tar archive) in the user's home directory.

    Parameters
    ----------
    fp : str
        File path of the project to restore.
    project_name : str, optional
        Name of new project to create
    overwrite_existing : bool, optional

    Returns
    -------
    project_name : str
        Name of the project that was restored.

    Raises
    ------
    ValueError
       If the project does not exist.

    See Also
    --------
    bw2io.backup.backup_project_directory: To restore a project directory from a backup.
    """

    def get_project_name(fp):
        reader = codecs.getreader("utf-8")
        # See https://stackoverflow.com/questions/68997850/python-readlines-with-tar-file-gives-streamerror-seeking-backwards-is-not-al/68998071#68998071
        with tarfile.open(fp, "r:gz") as tar:
            for member in tar:
                if member.name[-17:] == "project-name.json":
                    return json.load(reader(tar.extractfile(member)))["name"]
            raise ValueError("Couldn't find project name file in archive")

    assert os.path.isfile(fp), "Can't find file at path: {}".format(fp)
    print("Restoring project backup archive - this could take a few minutes...")
    project_name = get_project_name(fp) if project_name is None else project_name

    if project_name in projects and not overwrite_existing:
        raise ValueError("Project {} already exists".format(project_name))

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
        extracted_dir = [(Path(td) / dirname) for dirname in Path(td).iterdir() if (Path(td) / dirname).is_dir()]
        if not len(extracted_dir) == 1:
            raise ValueError("Can't find single directory extracted from project archive")
        extracted_path = extracted_dir[0]

        _current = projects.current
        projects.set_current(project_name, update=False)
        shutil.copytree(extracted_path, projects.dir, dirs_exist_ok=True)
        projects.set_current(_current)

    return project_name
