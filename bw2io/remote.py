from pathlib import Path
from typing import Optional,Union

import bw2data as bd
import requests
from platformdirs import user_data_dir

from .backup import restore_project_directory
from .download_utils import download_with_progressbar

PROJECTS_BW2 = {
    "ecoinvent-3.8-biosphere": "ecoinvent-3.8-biosphere.bw2.tar.gz",
    "ecoinvent-3.9.1-biosphere": "ecoinvent-3.9.1-biosphere.bw2.tar.gz",
}

PROJECTS_BW25 = {
    "ecoinvent-3.8-biosphere": "ecoinvent-3.8-biosphere.tar.gz",
    "ecoinvent-3.9.1-biosphere": "ecoinvent-3.9.1-biosphere.tar.gz",
    "USEEIO-1.1": "USEEIO-1.1.tar.gz",
}

cache_dir = Path(
    user_data_dir(appname="bw2io-project-cache", appauthor="brightway-team")
)
cache_dir.mkdir(exist_ok=True, parents=True)


def get_projects(update_config: Optional[bool] = True) -> dict:
    BW2 = bd.__version__ < (4,)
    projects = PROJECTS_BW2 if BW2 else PROJECTS_BW25
    URL = "https://files.brightway.dev/"
    FILENAME = "projects-config.bw2.json" if BW2 else "projects-config.json"
    if update_config:
        try:
            projects = requests.get(URL + FILENAME).json()
        except:
            pass
    return projects


def install_project(
    project_key: str,
    project_name: Optional[str] = None,
    projects_config: Optional[dict] = get_projects(),
    url: Optional[str] = "https://files.brightway.dev/",
    overwrite_existing: Optional[bool] = False,
    __recursive: Union[bool,None] = False
):
    """
    Install an existing Brightway project archive.

    By default uses ``https://files.brightway.dev/`` as the file repository, but you can run your own.

    Parameters
    ----------
    project_key: str
        A string uniquely identifying a project, e.g. ``ecoinvent-3.8-biosphere``.
    project_name: str, optional
        The name of the new project to create. If not provided will be taken from the archive file.
    projects_config: dict, optional
        A dictionary that maps ``project_key`` values to filenames at the repository
    url: str, optional
        The URL, with trailing slash ``/``, where the file can be found.
    overwrite_existing: bool, optional
        Allow overwriting an existing project
    __recursive : bool
        Internal flag used to determine if this function has errored out already

    Returns
    -------
    str
        The name of the created project.
    """
    try:
        filename = projects_config[project_key]
    except KeyError:
        raise KeyError(f"Project key {project_key} not in `project_config`")

    fp = cache_dir / filename
    if not fp.exists():
        download_with_progressbar(
            url=url + filename, filename=filename, dirpath=cache_dir
        )

    try:
        return restore_project_directory(
            fp=fp, project_name=project_name, overwrite_existing=overwrite_existing
        )
    except EOFError:
        # Corrupt or incomplete zip archive
        fp.unlink()
        if __recursive:
            raise OSError("Multiple errors trying to download and extract this file. Better luck tomorrow?")
        else:
            return install_project(
                project_key=project_key,
                project_name=project_name,
                projects_config=projects_config,
                url=url,
                overwrite_existing=overwrite_existing,
                __recursive=True,
            )
