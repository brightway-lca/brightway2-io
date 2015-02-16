from bw2data import mapping
from ..utils import activity_hash


def add_cf_biosphere_activity_hash(data, biosphere_db_name):
    for method in data:
        for cf in method['data']:
            if cf.get("code"):
                continue
            key = activity_hash(cf)
            if (biosphere_db_name, key) in mapping:
                cf['code'] = key
    return data
