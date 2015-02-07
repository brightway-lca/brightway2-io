from bw2data import mapping

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
        if len(amounted) == 1:
            ds[u'reference product'] = amounted[0]['name']
            data[u'flow'] = amounted[0][u'flow']
            if not data.get('unit'):
                ds[u'unit'] = amounted[0]['unit']
            ds[u'production amount'] = amounted[0]['amount']
    return db
