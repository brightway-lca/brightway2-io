# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2io.data import get_valid_geonames


def test_geodata():
    assert 'WECC, US only' in get_valid_geonames()
    assert 'Québec, HQ distribution network' in get_valid_geonames()
