# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

class InvalidPackage(Exception):
    """bw2package data doesn't validate"""
    pass


class UnsafeData(Exception):
    """bw2package data comes from a class that isn't recognized by Brightway2"""
    pass


class UnsupportedExchange(Exception):
    """This exchange uncertainty type can't be rescaled automatically"""
    pass


class StrategyError(Exception):
    """The strategy could not be applied"""
    pass


class NonuniqueCode(Exception):
    """Not all provided codes are unique"""
    pass


class WrongDatabase(Exception):
    """Dataset does not belong to this database"""
    pass
