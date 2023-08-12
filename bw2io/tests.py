from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict


class MockMetadata(SerializedDict):
    filename = "mock-meta.json"


mocks = MockMetadata()


class MockDS(DataStore):
    """
    Mock DataStore for testing

    Methods
    -------
    process_data(row)
        Returns a tuple of empty lists and 0
    """

    _metadata = mocks
    validator = lambda x, y: True
    dtype_fields = []

    def process_data(self, row):
        return (), 0
