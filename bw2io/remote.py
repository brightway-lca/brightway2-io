from packaging.version import parse as vparse
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin

import bw2data as bd

from .backup import restore_project_directory
from .download_utils import download_with_progressbar

PROJECTS_BW2 = {
    "ecoinvent-3.8-biosphere": "ecoinvent-3.8-biosphere.bw2.tar.gz",
    "ecoinvent-3.9.1-biosphere": "ecoinvent-3.9.1-biosphere.bw2.tar.gz",
    "ecoinvent-3.10-biosphere": "ecoinvent-3.10-biosphere.bw2.tar.gz",
}

PROJECTS_BW25 = {
    "ecoinvent-3.8-biosphere": "ecoinvent-3.8-biosphere.tar.gz",
    "ecoinvent-3.9.1-biosphere": "ecoinvent-3.9.1-biosphere.tar.gz",
    "USEEIO-1.1": "USEEIO-1.1.tar.gz",
    "forwast": "forwast.tar.gz",
}

BASE_URL = "https://files.brightway.dev/"

cache_dir = Path(bd.projects._base_data_dir) / "bw2io_cache_dir"
cache_dir.mkdir(exist_ok=True)

def _projects_config_filename(version) -> str:
    """Return the config filename given a bd.__version__ (str or tuple)."""
    # Normalize tuple -> string
    if isinstance(version, tuple):
        version = ".".join(map(str, version))
    return "projects-config.json" if vparse(str(version)) >= vparse("4") else "projects-config.bw2.json"


def _fetch_projects_config(base_url: str, filename: str) -> dict:
    """Indirection point for I/O (easy to mock in tests)."""
    import requests  # local import so tests don’t even need requests if they patch this
    try:
        response = requests.get(urljoin(base_url, filename), timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        print(f"Can't connect to {base_url}: {exc}")
    except ValueError as exc:
        # JSON decoding error
        print(f"Invalid JSON received from {base_url}: {exc}")


def get_projects(update_config: bool = True, base_url: str = BASE_URL) -> dict:
    bd_version = bd.__version__
    if not isinstance(bd_version, str):
        bd_version = ".".join(map(str, bd_version))
    BW2 = vparse(bd_version) < vparse("4")
    projects = PROJECTS_BW2 if BW2 else PROJECTS_BW25
    if update_config:
        filename = _projects_config_filename(getattr(bd, "__version__", "0"))
        projects.update(_fetch_projects_config(base_url, filename))

    return projects


def install_project(
    project_key: str,
    project_name: Optional[str] = None,
    projects_config: Optional[dict] = None,
    url: Optional[str] = BASE_URL,
    overwrite_existing: Optional[bool] = False,
    __recursive: Union[bool, None] = False,
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
    if projects_config is None:
        projects_config = get_projects(base_url=url)

    try:
        filename = projects_config[project_key]
    except KeyError:
        raise KeyError(f"Project key {project_key} not in `projects_config`")

    fp = cache_dir / filename
    if not fp.exists():
        download_with_progressbar(
            url=urljoin(url, filename), filename=filename, dirpath=cache_dir
        )

    try:
        return restore_project_directory(
            fp=fp, project_name=project_name, overwrite_existing=overwrite_existing
        )
    except EOFError:
        # Corrupt or incomplete zip archive
        fp.unlink()
        if __recursive:
            raise OSError(
                "Multiple errors trying to download and extract this file. Better luck tomorrow?"
            )
        else:
            return install_project(
                project_key=project_key,
                project_name=project_name,
                projects_config=projects_config,
                url=url,
                overwrite_existing=overwrite_existing,
                __recursive=True,
            )
