from bw2data import Database
from bw2data.tests import bw2test
from bw2io import SingleOutputEcospold2Importer
from bw2io.errors import MultiprocessingError
import os
import pytest


FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures", "ecospold2")


@bw2test
def test_importer_custom_extractor():
    class Extractor:
        def __init__(self):
            self.data = []

        def extract(self, *args, **kwargs):
            self.data.append(args)
            return []

    ext = Extractor()

    imp = SingleOutputEcospold2Importer(FIXTURES, "ei", extractor=ext)
    assert imp.data == []
    assert ext.data == [(FIXTURES, "ei")]


@bw2test
def test_importer_mp_error():
    class Extractor:
        def __init__(self):
            pass

        def extract(self, *args, **kwargs):
            raise RuntimeError

    ext = Extractor()
    with pytest.raises(MultiprocessingError):
        SingleOutputEcospold2Importer(FIXTURES, "ei", extractor=ext)


@bw2test
def test_importer_signals():
    class SignalCatcher:
        def __init__(self):
            self.messages = []

        def emit(self, *args):
            self.messages.append(args)

    catcher = SignalCatcher()

    bio = Database("biosphere3")
    bio.write({})

    imp = SingleOutputEcospold2Importer(FIXTURES, "ei", signal=catcher)
    imp.apply_strategies()

    assert catcher.messages == [(i, 21) for i in range(1, 22)]
