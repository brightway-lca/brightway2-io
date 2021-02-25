from bw2data import projects
from bw2data.data_store import DataStore
from bw2data.serialization import SerializedDict, JsonWrapper
from .data import (
    get_biosphere_2_3_category_migration_data,
    get_biosphere_2_3_name_migration_data,
    get_exiobase_biosphere_migration_data,
    get_simapro_ecoinvent_3_migration_data,
    get_simapro_water_migration_data,
    get_us_lci_migration_data,
    get_ecoinvent_pre35_migration_data,
)
from .units import (
    get_default_units_migration_data,
    get_unusual_units_migration_data,
)
import os


class _Migrations(SerializedDict):
    filename = "migrations.json"


migrations = _Migrations()


class Migration(DataStore):
    _metadata = migrations

    def __init__(self, *args, **kwargs):
        super(Migration, self).__init__(*args, **kwargs)
        self._intermediate_dir = projects.request_directory("migrations")

    @property
    def description(self):
        return self.metadata["description"]

    def validate(self, *args, **kwargs):
        return

    def write(self, data, description):
        """Write migration data. Requires a description."""
        try:
            self.register()
            migrations[self.name]["description"] = description
        except:
            self.register(description=description)
        filepath = os.path.join(self._intermediate_dir, self.filename + ".json")
        JsonWrapper.dump(data, filepath)

    def load(self):
        self.register()
        filepath = os.path.join(self._intermediate_dir, self.filename + ".json")
        return JsonWrapper.load(filepath)


def create_core_migrations():
    """Add pre-defined core migrations data files"""
    Migration("biosphere-2-3-categories").write(
        get_biosphere_2_3_category_migration_data(),
        "Change biosphere category and subcategory labels to ecoinvent version 3",
    )
    Migration("biosphere-2-3-names").write(
        get_biosphere_2_3_name_migration_data(),
        "Change biosphere flow names to ecoinvent version 3",
    )
    Migration("simapro-ecoinvent-3.1").write(
        get_simapro_ecoinvent_3_migration_data("3.1"),
        "Change SimaPro names from ecoinvent 3.1 to ecoinvent names",
    )
    Migration("simapro-ecoinvent-3.2").write(
        get_simapro_ecoinvent_3_migration_data("3.2"),
        "Change SimaPro names from ecoinvent 3.2 to ecoinvent names",
    )
    Migration("simapro-ecoinvent-3.3").write(
        get_simapro_ecoinvent_3_migration_data("3.3"),
        "Change SimaPro names from ecoinvent 3.3 to ecoinvent names",
    )
    Migration("simapro-ecoinvent-3.4").write(
        get_simapro_ecoinvent_3_migration_data("3.4"),
        "Change SimaPro names from ecoinvent 3.4 to ecoinvent names",
    )
    Migration("simapro-ecoinvent-3.5").write(
        get_simapro_ecoinvent_3_migration_data("3.5"),
        "Change SimaPro names from ecoinvent 3.5 to ecoinvent names",
    )
    Migration("simapro-water").write(
        get_simapro_water_migration_data(),
        "Change SimaPro water flows to more standard names",
    )
    Migration("us-lci").write(
        get_us_lci_migration_data(), "Fix names in US LCI database"
    )
    Migration("default-units").write(
        get_default_units_migration_data(), "Convert to default units"
    )
    Migration("unusual-units").write(
        get_unusual_units_migration_data(), "Convert non-Ecoinvent units"
    )
    Migration("exiobase-biosphere").write(
        get_exiobase_biosphere_migration_data(),
        "Change biosphere flow names to ecoinvent version 3",
    )
    Migration("fix-ecoinvent-flows-pre-35").write(
        get_ecoinvent_pre35_migration_data(),
        "Update new biosphere UUIDs in Consequential 3.4",
    )
