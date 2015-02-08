from __future__ import print_function
from ..utils import format_for_logging, es2_activity_hash
from bw2data import mapping
from bw2data.logs import get_io_logger, close_log


def link_biosphere_by_flow_uuid(db, biosphere="biosphere3"):
    for ds in db:
        for exc in ds.get('exchanges', []):
            if exc.get('type') == u"biosphere" and exc.get("flow"):
                key = (biosphere, exc.get("flow"))
                if key in mapping:
                    exc[u"input"] = key
    return db


def remove_zero_amount_coproducts(db):
    """Remove coproducts with zero production amounts"""
    for ds in db:
        if ds.get('products', []):
            ds['products'] = [obj for obj in ds['products'] if obj.get('amount')]
    return db


def es2_assign_only_production_with_amount_as_reference_product(db):
    """If a multioutput process has one product with a non-zero amount, assign that product as reference product"""
    for ds in db:
        amounted = [prod for prod in ds['products'] if prod['amount']]
        # OK if it overwrites existing reference product; need flow as well
        if len(amounted) == 1:
            ds[u'reference product'] = amounted[0]['name']
            ds[u'flow'] = amounted[0][u'flow']
            if not ds.get('unit'):
                ds[u'unit'] = amounted[0]['unit']
            ds[u'production amount'] = amounted[0]['amount']
    return db


def create_composite_code(db):
    """Create composite code from activity and flow names"""
    for ds in db:
        ds['code'] = es2_activity_hash(ds['activity'], ds['flow'])
    return db


def link_internal_technosphere_by_composite_code(db):
    candidates = {ds['code'] for ds in db}
    for ds in db:
        for exc in ds.get('exchanges', []):
            if (exc['type'] in {'technosphere', 'production', 'substitution'}
                    and exc.get('activity')):
                key = es2_activity_hash(exc['activity'], exc['flow'])
                if key in candidates:
                    exc[u"input"] = (ds['database'], key)
    return db


def delete_exchanges_missing_activity(db):
    """Delete exchanges that weren't linked correctly by ecoinvent.

    These exchanges are missing the "activityLinkId" attribute, and the flow they want to consume is not produced as the reference product of any activity. See the `known data issues <http://www.ecoinvent.org/database/ecoinvent-version-3/reports-of-changes/known-data-issues/>`__ report.

    """
    log, logfile = get_io_logger("Ecospold2-import-error")
    print_warning = False
    for ds in db:
        exchanges = ds.get('exchanges', [])
        if not exchanges:
            continue
        skip = []
        for exc in exchanges:
            if (not exc.get('activity') and exc['type'] in
                    {'technosphere', 'production', 'substitution'}):
                log.critical(u"Purging unlinked exchange:\nFilename: {}\n{}"\
                    .format(ds[u'filename'], format_for_logging(exc)))
                print_warning = True
                skip.append(exc)
        ds[u'exchanges'] = [exc for exc in exchanges if exc not in skip]
    close_log(log)
    if print_warning:
        print(u"Some exchanges couldn't be linked and were delted. See the "
            u"logfile for details:\n\t{}".format(logfile))
    return db


def delete_ghost_exchanges(db):
    """Delete technosphere which can't be linked due to ecoinvent errors.

    A ghost exchange is one without an ``input``."""
    # TODO: Log ghost exchanges
    pass


    # for exc in [x for x in value[u'exchanges']
    #             if x[u'input'] not in mapping]:
    #     rewrite = True
    #     self.log.critical(
    #         u"Purging unlinked exchange:\nFilename: %s\n%s" %
    #         (value[u'linking'][u'filename'],
    #          pprint.pformat(exc, indent=2))
    #     )

