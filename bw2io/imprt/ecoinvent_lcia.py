from .base import ImportBase
from ..data import convert_lcia_methods_data
from ..strategies import (
)

class EcoinventLCIAImporter(ImportBase):
    # strategies = [
    #     functools.partial(fill_in_strategy_name,
    #         db_nameconfig.biosphere,
    #         fields=('name', 'categories')
    #     ),
    # ]

    def __init__(self):
        self.csv_data, self.cf_data, self.file = convert_lcia_methods_data()
        self.separate_methods()

    def separate_methods(self):
        methods = {obj['method'] for obj in self.cf_data}
        metadata = {obj.pop('name'): obj for obj in self.csv_data}
        self.data = [{
            u'filename': self.file,
            u'name': method,
            u'exchanges': [{
                u'name': cf['name'],
                u'categories': cf['categories'],
                u'amount': cf['amount']}
            for cf in self.cf_data
            if cf['method'] == method]
        } for method in methods]
        for obj in self.data:
            obj.update(**metadata.get(obj['name'], {}))
