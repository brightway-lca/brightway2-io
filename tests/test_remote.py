import pytest
import bw2data as bd
from bw2io import remote


# What requests.get(...).json() should return.
PROJECTS = {
    'ecoinvent-3.8-biosphere': 'ecoinvent-3.8-biosphere.tar.gz',
    'ecoinvent-3.9.1-biosphere': 'ecoinvent-3.9.1-biosphere.tar.gz',
    'USEEIO-1.1': 'USEEIO-1.1.tar.gz',
    'forwast': 'forwast.tar.gz',
    'ecoinvent-3.10-biosphere': 'ecoinvent-3.10-biosphere.tar.gz',
    'regionalization-example': 'regionalization-example.tar.gz',
    'spatiotemporal-example': 'spatiotemporal-example.tar.gz',
    'multifunctional-demo': 'multifunctional-demo.tar.gz'
}


def test_get_projects_handles_string_version(mocker, monkeypatch):
    """
    Test for fix of issue: https://github.com/brightway-lca/brightway2-io/issues/326

    Test that get_projects:
    1. Correctly parses the bd.__version__ string.
    2. Calls the correct URL based on the version.
    3. Returns the updated dictionary.
    """
    
    # Mock the base project constants as empty dicts.
    mocker.patch("bw2io.remote.PROJECTS_BW2", {})
    mocker.patch("bw2io.remote.PROJECTS_BW25", {})

    # Create a mock for the 'requests.get' response object
    mock_response = mocker.Mock()
    # Configure it to return the real data you provided
    mock_response.json.return_value = PROJECTS
    
    # Patch 'requests.get' inside the bw2io.remote module
    mock_get_patch = mocker.patch(
        "bw2io.remote.requests.get", 
        return_value=mock_response
    )

    # Test with version string "3.9.1" (< 4)
    # This should trigger the BW2 (v3) logic
    monkeypatch.setattr(bd, "__version__", "3.9.1")
    
    projects = remote.get_projects(update_config=True)
    
    # Assertions for v3
    assert projects == PROJECTS
    mock_get_patch.assert_called_with(
        "https://files.brightway.dev/projects-config.bw2.json"
    )

    # Test with version string "4.0.1" (>= 4)
    # This should trigger the BW25 (v4) logic
    monkeypatch.setattr(bd, "__version__", "4.0.1")

    projects = remote.get_projects(update_config=True)
    
    # Assertions for v4
    assert projects == PROJECTS
    mock_get_patch.assert_called_with(
        "https://files.brightway.dev/projects-config.json"
    )