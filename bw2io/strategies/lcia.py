from bw2data import mapping
from ..utils import activity_hash


def link_cf_by_activity_hash(cfs, biosphere=u"biosphere"):
    for cf in cfs:
        if cf.get('link'):
            continue
        else:
            key = (biosphere, activity_hash(cf['flow']))
            if key in mapping:
                cf[u"link"] = key
    return cfs
