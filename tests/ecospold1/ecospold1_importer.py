from bw2data.tests import bw2test
from bw2io import SingleOutputEcospold1Importer
from bw2io.errors import MultiprocessingError
import pytest


@bw2test
def test_importer_mp_error(tmpdir):
    class Extractor:
        def __init__(self):
            pass

        def extract(self, *args, **kwargs):
            raise RuntimeError

    ext = Extractor()
    with pytest.raises(MultiprocessingError):
        SingleOutputEcospold1Importer(tmpdir, 'foo', extractor=ext)
