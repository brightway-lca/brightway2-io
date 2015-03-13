from bw2data import config
from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict, JsonWrapper
import os


class _Migrations(SerializedDict):
    filename = "migrations.json"


migrations = _Migrations()


class Migration(DataStore):
    metadata = migrations
    _intermediate_dir = config.request_dir("migrations")

    def validate(self, *args, **kwargs):
        return

    def process(self):
        return

    def write(self, data):
        try:
            self.assert_registered()
        except:
            self.register()
        filepath = os.path.join(
            config.dir,
            self._intermediate_dir,
            self.filename + u".json"
        )
        JsonWrapper.dump(data, filepath)

    def load(self):
        self.assert_registered()
        filepath = os.path.join(
            config.dir,
            self._intermediate_dir,
            self.filename + u".json"
        )
        return JsonWrapper.load(filepath)

