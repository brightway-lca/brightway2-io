from ..strategies import link_cf_by_activity_hash
from .base import ImportBase
from time import time
import os


class Ecospold1LCIAImporter(object):
    def import_file(self, filepath):
        pass
        start = time()
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))

    def import_dir(self, dirpath):
        assert os.path.isdir(dirpath), "Must pass path to a directory"

        start = time()
        self.data = Ecospold1DataExtractor.extract(filepath, db_name)
        print(u"Extracted {} datasets in {:.2f} seconds".format(
              len(self.data), time() - start))
