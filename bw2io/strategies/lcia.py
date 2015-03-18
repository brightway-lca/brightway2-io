from bw2data import mapping, Database
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
    """For a set of top-level (i.e. only one category deep) CFs, add CFs for all existing subcategories.

    If ``remove``, also delete the top-level CF if it is unlinked."""
    def add_amount(obj, amount):
        obj['amount'] = amount
        return obj

    def add_subcategories(obj):
        # Sorting needed for tests
        new_objs = sorted(mapping[(
            obj['categories'][0],
            obj['name'],
            obj['unit'],
        )])
        return [add_amount(elem, obj['amount']) for elem in new_objs]

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
                'categories': flow.categories,
                'database': flow.database,
                'input': flow.key,
                'name': flow.name,
                'unit': flow.unit,
            })

    only_top_level_categories = lambda x: all([len(y.get('categories', [])) == 1
                                               for y in x])

    for method in data:
        if not only_top_level_categories(method['exchanges']):
            continue
        new_cfs = []
        for obj in method['exchanges']:
            subcat_cfs = add_subcategories(obj)
            if subcat_cfs and remove:
                obj['remove_me'] = True
            new_cfs.extend(subcat_cfs)
        method[u'exchanges'].extend(new_cfs)
        if remove:
            method[u'exchanges'] = [obj for obj in method['exchanges']
                                    if not obj.get('remove_me')]
    return data
