from bw2data import Database
from bw2data.tests import bw2test
from bw2io import SingleOutputEcospold2Importer
import os


FIXTURES = os.path.join(os.path.dirname(__file__), "..", "fixtures", "ecospold2")


@bw2test
def test_importer_custom_extractor():
    class Extractor:
        def __init__(self):
            self.data = []

        def extract(self, *args):
            self.data.append(args)
            return []

    ext = Extractor()

    imp = SingleOutputEcospold2Importer(FIXTURES, 'ei', extractor=ext)
    assert imp.data == []
    assert ext.data == [(FIXTURES, 'ei')]


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

    imp = SingleOutputEcospold2Importer(FIXTURES, 'ei', signal=catcher)
    imp.apply_strategies()

    assert catcher.messages == [(i, 19) for i in range(1, 20)]
