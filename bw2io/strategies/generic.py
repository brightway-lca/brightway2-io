from bw2data import mapping
from ..utils import activity_hash


def link_biosphere_by_activity_hash(db, biosphere=u"biosphere"):
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc.get('biosphere'):
                key = (biosphere, activity_hash(exc))
                if key in mapping:
                    exc[u"input"] = key
    return db
