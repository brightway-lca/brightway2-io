from ..utils import activity_hash


def add_cf_biosphere_activity_hash(data):
    for method in data:
        for cf in method['data']:
            if cf.get("code"):
                continue
            cf['code'] = activity_hash(cf)
    return data
