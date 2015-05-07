# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import Database
from ..utils import activity_hash
import collections
import copy


def add_activity_hash_code(data):
    """Add ``code`` field to characterization factors using ``activity_hash``, if ``code`` not already present."""
    for method in data:
        for cf in method['exchanges']:
            if cf.get("code"):
                continue
            cf[u'code'] = activity_hash(cf)
    return data


def drop_unlinked_cfs(data):
    """Drop CFs which don't have ``input`` attribute"""
    for method in data:
        method[u'exchanges'] = [cf for cf in method['exchanges']
                                if cf.get('input') is not None]
    return data


def set_biosphere_type(data):
    """Set CF types to 'biosphere', to keep compatibility with LCI strategies.

    This will overwrite existing ``type`` values."""
    for method in data:
        for cf in method['exchanges']:
            cf[u'type'] = u'biosphere'
    return data


def match_subcategories(data, biosphere_db_name, remove=True):
    """Given a characterization with a top-level category, e.g. ``('air',)``, find all biosphere flows with the same top-level categories, and add CFs for these flows as well. Doesn't replace CFs for existing flows with multi-level categories. If ``remove``, also delete the top-level CF, but only if it is unlinked."""
    def add_amount(obj, amount):
        obj['amount'] = amount
        return obj

    def add_subcategories(obj, mapping):
        # Sorting needed for tests
        new_objs = sorted(mapping[(
            obj['categories'][0],
            obj['name'],
            obj['unit'],
        )], key=lambda x: tuple([x[key] for key in sorted(x.keys())]))
        # Need to create copies so data from later methods doesn't
        # clobber amount values
        return [add_amount(copy.deepcopy(elem), obj['amount'])
                for elem in new_objs]

    mapping = collections.defaultdict(list)
    for flow in Database(biosphere_db_name):
        if not flow.get("type") == 'emission':
            continue
        if len(flow.get('categories', [])) > 1:
            mapping[(
                flow['categories'][0],
                flow['name'],
                flow['unit']
            )].append({
                'categories': flow['categories'],
                'database': flow['database'],
                'input': flow.key,
                'name': flow['name'],
                'unit': flow['unit'],
            })

    for method in data:
        already_have = {(obj['name'], obj['categories']) for obj in method['exchanges']}

        new_cfs = []
        for obj in method['exchanges']:
            if len(obj['categories']) > 1:
                continue
            # Don't add subcategory flows which already have CFs
            subcat_cfs = [x for x in add_subcategories(obj, mapping)
                          if (x['name'], x['categories']) not in already_have]
            if subcat_cfs and remove and not obj.get('input'):
                obj['remove_me'] = True
            new_cfs.extend(subcat_cfs)
        method[u'exchanges'].extend(new_cfs)
        if remove:
            method[u'exchanges'] = [obj for obj in method['exchanges']
                                    if not obj.get('remove_me')]
    return data
