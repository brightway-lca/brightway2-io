import pytest
import bw2data as bd
import bw2io as bi

PROJECTS = {
    "ecoinvent-3.8-biosphere": "ecoinvent-3.8-biosphere.tar.gz",
    "ecoinvent-3.9.1-biosphere": "ecoinvent-3.9.1-biosphere.tar.gz",
    "USEEIO-1.1": "USEEIO-1.1.tar.gz",
    "forwast": "forwast.tar.gz",
    "ecoinvent-3.10-biosphere": "ecoinvent-3.10-biosphere.tar.gz",
    "regionalization-example": "regionalization-example.tar.gz",
    "spatiotemporal-example": "spatiotemporal-example.tar.gz",
    "multifunctional-demo": "multifunctional-demo.tar.gz",
}

@pytest.mark.parametrize(
    "version, expected_filename",
    [
        ("3.9.1", "projects-config.bw2.json"),
        ((3, 9, 1), "projects-config.bw2.json"),
        ("4.0.1", "projects-config.json"),
        ((4, 0, 0), "projects-config.json"),
    ],
)
def test_get_projects_pure_unit_no_requests(monkeypatch, version, expected_filename):
    # Avoid any dependence on module constants
    monkeypatch.setattr(bi.remote, "PROJECTS_BW2", {})
    monkeypatch.setattr(bi.remote, "PROJECTS_BW25", {})

    # Set version as str or tuple
    monkeypatch.setattr(bd, "__version__", version)

    # Capture what get_projects *would* fetch, but don’t do I/O
    seen = {}
    def fake_fetch(base_url, filename):
        seen["base_url"] = base_url
        seen["filename"] = filename
        return PROJECTS

    monkeypatch.setattr(bi.remote, "_fetch_projects_config", fake_fetch)

    projects = bi.remote.get_projects(update_config=True, base_url="https://files.brightway.dev/")

    assert projects == PROJECTS
    assert seen["base_url"] == "https://files.brightway.dev/"
    assert seen["filename"] == expected_filename

