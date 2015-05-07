# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict
from bw2data import config


config.request_dir("unlinked")


class _UnlinkedData(SerializedDict):
    filename = "unlinked_data.json"


unlinked_data = _UnlinkedData()


class UnlinkedData(DataStore):
    metadata = unlinked_data
    _intermediate_dir = 'unlinked'

    def validate(self, *args, **kwargs):
        return
