import os
import csv


class CSVExtractor(object):
    @classmethod
    def extract(cls, filepath, encoding="utf-8-sig"):
        assert os.path.exists(filepath), "Can't file file at path {}".format(filepath)
        with open(filepath, encoding=encoding) as f:
            reader = csv.reader(f)
            data = [row for row in reader]
        return [os.path.basename(filepath), data]
