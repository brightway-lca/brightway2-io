from bw2data.tests import bw2test
from bw2io import SingleOutputEcospold1Importer
from bw2io.errors import MultiprocessingError
import pytest
import os

FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures", "ecospold1")


@bw2test
def test_importer_mp_error(tmpdir):
    class Extractor:
        def __init__(self):
            pass

        def extract(self, *args, **kwargs):
            raise RuntimeError

    ext = Extractor()
    with pytest.raises(MultiprocessingError):
        SingleOutputEcospold1Importer(tmpdir, "foo", extractor=ext)


@bw2test
def test_ecospold1_extractor_working():
    ei = SingleOutputEcospold1Importer(
        os.path.join(
            FIXTURES,
            "Acrylonitrile-butadiene-styrene copolymer (ABS), resin, at plant CTR.xml",
        ),
        "foo",
    )
    assert ei.data


@bw2test
def test_ecospold1_extractor_invalid_tag():
    ei = SingleOutputEcospold1Importer(
        os.path.join(FIXTURES, "Acetic acid, at plant.xml"), "foo"
    )
    assert not ei.data


@bw2test
def test_ecospold1_extractor_missing_tag():
    ei = SingleOutputEcospold1Importer(
        os.path.join(FIXTURES, "Aluminum, extrusion, at plant.xml"), "foo"
    )
    assert not ei.data
