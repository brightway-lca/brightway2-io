# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

import os
import csv


class CSVExtractor(object):
    @classmethod
    def extract(cls, filepath):
        assert os.path.exists(filepath), "Can't file file at path {}".format(filepath)
        with open(filepath) as f:
            reader = csv.reader(f)
            data = [row for row in reader]
        return data
