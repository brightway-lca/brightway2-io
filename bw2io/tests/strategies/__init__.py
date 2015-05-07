# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from .allocation import Ecospold1AllocationTestCase
from .biosphere import (
    BiosphereCategoryNormalizationTestCase,
    BiosphereLinkingTestCase,
    BiosphereNameNormalizationTestCase,
    UnspecifiedCategoryTestCase,
)
from .generic import GenericStrategiesTestCase
from .lcia import LCIATestCase, LCIATestCase2
from .link_iterable import LinkIterableTestCase
from .simapro_name_splitting import NameSplittingTestCase
from .simapro_normalization import SPNormalizationTestCase
