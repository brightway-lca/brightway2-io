import codecs
import datetime
import json
import os
import tarfile

from bw2data import projects
from bw_processing import safe_filename


def backup_data_directory():
    # """Backup data directory to a ``.tar.gz`` (compressed tar archive).

    # Backup archive is saved to the user's home directory.

    # Restoration is done manually. Returns the filepath of the backup archive."""
    """
    Backup data directory to a ``.tar.gz`` (compressed tar archive) in the user's home directory.

    Restoration is done manually.

    Returns
    -------
    str
        Filepath of the backup archive.

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


def backup_project_directory(project):
    # """Backup project data directory to a ``.tar.gz`` (compressed tar archive).

    # ``project`` is the name of a project.

    # Backup archive is saved to the user's home directory.

    # Restoration is done using ``restore_project_directory``.

    # Returns the filepath of the backup archive."""
    """
    Backup project data directory to a ``.tar.gz`` (compressed tar archive) in the user's home directory.

    Parameters
    ----------
    project : str
        Name of a project to backup.
    
    Returns
    -------
    str
        Filepath of the backup archive.

    Raises
    ------
    ValueError
        If the project does not exist.

    Examples
    --------
    >>> bw2io.bw2setup()
    >>> bw2io.backup.backup_project_directory('default')
    Creating project backup archive - this could take a few minutes...

    See Also
    --------
    bw2io.backup.restore_project_directory: Restore a project backup.
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


def restore_project_directory(fp):
    # """
    # Restore backup created using ``backup_project_directory``.

    # Raises an error is the project already exists.

    # ``fp`` is the filepath of the backup archive.

    # Returns the name of the newly created project.
    # """
    """
    Restore backup created using ``backup_project_directory``.

    Parameters
    ----------
    fp : str
        Filepath of the backup archive.

    Returns
    -------
    str
        Name of the newly created project.

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
        with tarfile.open(fp, "r|gz") as tar:
            for member in tar:
                if member.name[-17:] == "project-name.json":
                    return json.load(reader(tar.extractfile(member)))["name"]
            raise ValueError("Couldn't find project name file in archive")

    assert os.path.isfile(fp), "Can't find file at path: {}".format(fp)
    print("Restoring project backup archive - this could take a few minutes...")
    project_name = get_project_name(fp)

    with tarfile.open(fp, "r|gz") as tar:
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
            
        
        safe_extract(tar, projects._base_data_dir)

    _current = projects.current
    projects.set_current(project_name, update=False)
    projects.set_current(_current)
    return project_name
