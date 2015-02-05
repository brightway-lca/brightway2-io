def allocate_ecospold1_datasets(data):
    """This strategy allocates multioutput datasets to new datasets.

    This deletes the multioutput dataset, breaking any existing linking. This shouldn't be a concern, as you shouldn't link to a multioutput dataset in any case.

    """
    pass

    def allocate_datasets(self, data):
        activities = []
        for ds in data:
            multi_output = [exc for exc in ds[u"exchanges"] \
                if u"reference" in exc]
            if multi_output:
                for activity in self.allocate_exchanges(ds):
                    activities.append(activity)
            else:
                activities.append(ds)
        return activities

    def allocate_exchanges(self, ds):
        """
Take a dataset, which has multiple outputs, and return a list of allocated datasets.

Two things change in the allocated datasets. First, the name changes to the names of the individual outputs. Second, the list of exchanges is rewritten, and only the allocated exchanges are used.

        """
        coproduct_codes = [exc[u"code"] for exc in ds[u"exchanges"] if exc.get(
            u"group", None) in (0, 2)]
        coproducts = dict([(x, copy.deepcopy(ds)) for x in coproduct_codes])
        exchanges = dict([(exc[u"code"], exc) for exc in ds[u"exchanges"
            ] if u"code" in exc])
        allocations = [a for a in ds[u"exchanges"] if u"fraction" in a]
        # First, get production amounts for each coproduct.
        # these aren't included in the allocations
        for key, product in coproducts.iteritems():
            product[u"exchanges"] = [exc for exc in product[u"exchanges"] if exc.get(u"code", None) == key]
        # Next, correct names, location, and unit
        for key, product in coproducts.iteritems():
            for label in (u"unit", u"name", u"location"):
                if exchanges[key][u"matching"].get(label, None):
                    product[label] = exchanges[key][u"matching"][label]
        # Finally, add the allocated exchanges
        for allocation in allocations:
            if allocation[u"fraction"] == 0:
                continue
            product = coproducts[allocation[u"reference"]]
            for exc_code in allocation[u"exchanges"]:
                copied = copy.deepcopy(exchanges[exc_code])
                copied[u"amount"] = copied[u"amount"] * allocation[u"fraction"]
                product[u"exchanges"].append(copied)
        return coproducts.values()
