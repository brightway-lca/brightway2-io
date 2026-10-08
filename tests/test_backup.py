import json
import shutil

import pytest
from bw2data import Database, Method, databases, projects
from bw2data.tests import bw2test

from bw2io.backup import (
    _add_project_metadata,
    _extract_single_directory_tarball,
    _remove_project_metadata,
    _restore_project_metadata,
    backup_project_directory,
    restore_project_directory,
)


@pytest.fixture
@bw2test
def unsourced() -> None:
    projects.set_current("test-unsourced")
    projects.dataset.data["arbitrary"] = True
    Database("foo").write({})
    Method(("bar",)).register()
    Method(("bar",)).write([])


@pytest.fixture
@bw2test
def sourced() -> None:
    projects.set_current("test-sourced")
    projects.dataset.set_sourced()
    projects.dataset.data["arbitrary"] = True
    Database("foo").write({})
    Method(("bar",)).register()
    Method(("bar",)).write([])


def test_add_project_metadata_sourced(sourced):
    assert projects.current == "test-sourced"
    _add_project_metadata()
    assert (projects.dir / "project-metadata.json").is_file()
    metadata = json.load(open(projects.dir / "project-metadata.json"))
    print(metadata)
    assert "name" not in metadata
    assert metadata["revision"]
    assert metadata["is_sourced"]
    assert "full_hash" not in metadata
    assert metadata["data"]["arbitrary"]
    assert metadata["data"]["25"]


def test_add_project_metadata_unsourced(unsourced):
    assert projects.current == "test-unsourced"
    _add_project_metadata()
    assert (projects.dir / "project-metadata.json").is_file()
    metadata = json.load(open(projects.dir / "project-metadata.json"))
    assert "name" not in metadata
    assert "revision" not in metadata
    assert "is_sourced" not in metadata
    assert "full_hash" not in metadata
    assert metadata["data"]["arbitrary"]
    assert metadata["data"]["25"]


def test_remove_project_metadata(unsourced):
    assert projects.current == "test-unsourced"
    _add_project_metadata()
    assert (projects.dir / "project-metadata.json").is_file()
    _remove_project_metadata()
    assert not (projects.dir / "project-metadata.json").is_file()


def test_restore_project_metadata(sourced):
    assert projects.current == "test-sourced"
    _add_project_metadata()

    fp = projects.dir / "project-metadata.json"
    assert fp.is_file()

    projects.set_current("other")
    assert not projects.dataset.is_sourced
    assert not projects.dataset.revision
    assert not projects.dataset.data.get("arbitrary")

    shutil.copy(fp, projects.dir / "project-metadata.json")
    _restore_project_metadata()

    assert projects.dataset.is_sourced
    assert projects.dataset.revision
    assert projects.dataset.data["arbitrary"]


def test_backup_project(sourced, tmp_path):
    filepath = backup_project_directory(
        "test-sourced", timestamp=False, dir_backup=tmp_path
    )
    dirpath = _extract_single_directory_tarball(filepath, tmp_path)
    assert (dirpath / "project-metadata.json").is_file()
    assert (dirpath / "lci").is_dir()
    assert (dirpath / "revisions").is_dir()


def test_restore_project(sourced, tmp_path):
    filepath = backup_project_directory(
        "test-sourced", timestamp=False, dir_backup=tmp_path
    )
    revision = projects.dataset.revision

    projects.set_current("default")

    restore_project_directory(filepath, project_name="something-else", switch=True)
    assert projects.current == "something-else"
    assert projects.dataset.is_sourced
    assert projects.dataset.revision == revision


@bw2test
def test_backup_project_full_hash_directory(tmp_path):
    # Projects created before bw2data 4.x short hashes keep a full-hash directory name.
    projects.create_project("test-full-hash", full_hash=True)
    projects.set_current("test-full-hash")
    Database("foo").write({})

    filepath = backup_project_directory(
        "test-full-hash", timestamp=False, dir_backup=tmp_path
    )
    dirpath = _extract_single_directory_tarball(filepath, tmp_path)
    assert (dirpath / "lci").is_dir()
    assert (dirpath / "project-metadata.json").is_file()


def test_backup_project_not_current(sourced, tmp_path):
    projects.set_current("default")

    filepath = backup_project_directory(
        "test-sourced", timestamp=False, dir_backup=tmp_path
    )
    dirpath = _extract_single_directory_tarball(filepath, tmp_path)
    metadata = json.load(open(dirpath / "project-metadata.json"))
    assert metadata["is_sourced"]
    assert metadata["data"]["arbitrary"]


@bw2test
def test_restore_project_full_hash_directory(tmp_path):
    projects.create_project("test-full-hash", full_hash=True)
    projects.set_current("test-full-hash")
    Database("foo").write({})
    filepath = backup_project_directory(
        "test-full-hash", timestamp=False, dir_backup=tmp_path
    )

    restore_project_directory(filepath, project_name="restored", switch=True)
    assert projects.current == "restored"
    assert "foo" in databases
    assert not (projects.dir / "project-metadata.json").exists()


def test_restore_project_switch_loads_restored_databases(unsourced, tmp_path):
    filepath = backup_project_directory(
        "test-unsourced", timestamp=False, dir_backup=tmp_path
    )
    projects.set_current("default")

    restore_project_directory(filepath, project_name="restored", switch=True)
    assert "foo" in databases
