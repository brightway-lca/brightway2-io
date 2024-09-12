from bw2data import Database, get_node
from bw2data.tests import bw2test

from bw2io.importers import SimaProBlockCSVImporter


class Mock(SimaProBlockCSVImporter):
    # Mock to skip CSV extraction
    def __init__(self):
        return


@bw2test
def test_create_regionalized_biosphere_proxies():
    importer = Mock()
    importer.db_name = "database-name"
    importer.data = [
        {
            "exchanges": [
                {
                    "name": "a",
                    "unit": "kg",
                    "location": "BR",
                    "categories": ("air", "rural"),
                    "type": "biosphere",
                    "input": ("flows", "a"),
                },
                {
                    "name": "a",
                    "unit": "kg",
                    "location": "CL",
                    "categories": ("air", "rural"),
                    "type": "biosphere",
                    "input": ("flows", "a"),
                },
            ]
        }
    ]

    Database("flows").write(
        {
            ("flows", "a"): {
                "name": "a",
                "unit": "kg",
                "categories": ("air", "rural"),
                "type": "some type",
            }
        }
    )
    Database("proxies").write(
        {
            ("proxies", "a"): {
                "name": "a",
                "unit": "kg",
                "location": "BR",
                "categories": ("air", "rural"),
                "type": "some type",
            }
        }
    )

    importer.create_regionalized_biosphere_proxies("proxies")

    node = get_node(location="CL", database="proxies")
    assert importer.data[0]["exchanges"][0]["input"] == ("proxies", "a")
    assert importer.data[0]["exchanges"][1]["input"] == node.key
