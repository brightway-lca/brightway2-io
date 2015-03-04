from bw2data import mapping, Database
from ..utils import activity_hash
import collections


def add_activity_hash_code(data):
    """Add ``code`` field to characterization factors using ``activity_hash``, if ``code`` not already present."""
    for method in data:
        for cf in method['exchanges']:
            if cf.get("code"):
                continue
            cf[u'code'] = activity_hash(cf)
    return data


# def drop_unlinked_cfs(data):
#     for method in data:
#         method['exchanges'] = [cf for cf in method['exchanges'] if cf.get('code') is not None]
#     return data


def set_biosphere_type(data):
    """Set CF types to 'biosphere', to keep compatibility with LCI strategies"""
    for method in data:
        for cf in method['exchanges']:
            cf[u'type'] = u'biosphere'
    return data


def match_subcategories(data, biosphere_db_name):
    """For a set of top-level (i.e. only one category deep) CFs, try to match CFs to all existing subcategories.

    First, create a dict of biosphere hashes to categories. The hashes are computed using only the top-level category:

        flow_category_mapping = {"some-hash": [("cat 1", ('bio db', 'bio code'))]}

    For each method, skip the method if it has CFs for flows with subcategories. Otherwise, for each flow:

        * Skip the flow if it already has a code
        * Otherwise, rewrite the CF to match each existing (sub)category in the biosphere database.

    """
    def strip_subcategory(ds):
        if 'categories' in ds:
            ds['categories'] = ds['categories'][:1]
        return ds

    def rewrite_cf(cf, categories, key):
        cf.update(code=key[1], categories=categories)
        return cf

    flow_category_mapping = collections.defaultdict(list)
    for flow in Database(biosphere_db_name):
        flow_category_mapping[activity_hash(strip_subcategory(flow))
                              ].append((flow['categories'], flow.key))

    only_top_level_categories = lambda x: all([len(y['categories']) == 1
                                               for y in x])

    for method in data:
        if not only_top_level_categories(method['exchanges']):
            continue
        cfs = [cf for cf in method['exchanges'] if cf.get('code')] + \
              [cf for cf in method['exchanges']
               if activity_hash(cf) not in flow_category_mapping] + \
              [rewrite_cf(cf, categories, key)
               for cf in method['exchanges']
               for categories, key in flow_category_mapping.get(activity_hash(cf), [])
               if not cf.get('code')]
        method[u'exchanges'] = cfs
    return data
