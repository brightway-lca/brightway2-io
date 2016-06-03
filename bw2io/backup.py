# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import projects
from bw2data.filesystem import safe_filename
import codecs
import datetime
import json
import os
import tarfile


def backup_data_directory():
    """Backup data directory to a ``.tar.gz`` (compressed tar archive).

    Backup archive is saved to the user's home directory.

    Restoration is done manually. Returns the filepath of the backup archive."""
    fp = os.path.join(
        os.path.expanduser("~"),
        "brightway2-data-backup.{}.tar.gz".format(
            datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p")
        )
    )
    print("Creating backup archive - this could take a few minutes...")
    with tarfile.open(fp, "w:gz") as tar:
        tar.add(projects.dir, arcname=os.path.basename(projects.dir))


def backup_project_directory(project):
    """Backup project data directory to a ``.tar.gz`` (compressed tar archive).

    ``project`` is the name of a project.

    Backup archive is saved to the user's home directory.

    Restoration is done using ``restore_project_directory``.

    Returns the filepath of the backup archive."""
    if project not in projects:
        raise ValueError("Project {} does not exist".format(project))

    fp = os.path.join(
        os.path.expanduser("~"),
        "brightway2-project-{}-backup.{}.tar.gz".format(
            project,
            datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p")
        )
    )
    dir_path = os.path.join(
        projects._base_data_dir,
        safe_filename(project)
    )
    with open(os.path.join(dir_path, ".project-name.json"), "w") as f:
        json.dump({'name': project}, f)
    print("Creating project backup archive - this could take a few minutes...")
    with tarfile.open(fp, "w:gz") as tar:
        tar.add(
            dir_path,
            arcname=safe_filename(project)
        )

def restore_project_directory(fp):
    """Restore backup created using ``backup_project_directory``.

    Raises an error is the project already exists.

    ``fp`` is the filepath of the backup archive.

    Returns the name of the newly created project."""
    def get_project_name(fp):
        reader = codecs.getreader("utf-8")
        with tarfile.open(fp, 'r|gz') as tar:
            for member in tar:
                if member.name[-17:] == "project-name.json":
                    return json.load(reader(tar.extractfile(member)))['name']
            raise ValueError("Couldn't find project name file in archive")

    assert os.path.isfile(fp), "Can't find file at path: {}".format(fp)
    print("Restoring project backup archive - this could take a few minutes...")
    project_name = get_project_name(fp)

    with tarfile.open(fp, 'r|gz') as tar:
        tar.extractall(projects._base_data_dir)

    _current = projects.current
    projects.set_current(project_name, update=False)
    projects.set_current(_current)
    return project_name
