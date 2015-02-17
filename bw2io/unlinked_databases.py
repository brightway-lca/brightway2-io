from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict
from bw2data import config


config.request_dir(u"unlinked")


class _UnlinkedDatabases(SerializedDict):
    filename = "unlinked_databases.json"


unlinked_databases = _UnlinkedDatabases()


class UnlinkedDatabase(DataStore):
    metadata = unlinked_databases
    _intermediate_dir = u'unlinked'

    def add_mappings(self, *args, **kwargs):
        return

    def validate(self, *args, **kwargs):
        return

    def process(self):
        return
