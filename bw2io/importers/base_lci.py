import collections
import functools
import itertools
import warnings
from typing import Optional, Tuple

from bw2data import Database, config, databases, labels, parameters
from bw2data.data_store import ProcessedDataStore
from bw2data.parameters import (
    ActivityParameter,
    DatabaseParameter,
    ParameterizedExchange,
    ProjectParameter,
)

from ..errors import NonuniqueCode, StrategyError, WrongDatabase
from ..export.excel import write_lci_matching
from ..migrations import migrations
from ..strategies import (
    assign_only_product_as_production,
    drop_unlinked,
    drop_unspecified_subcategories,
    link_iterable_by_fields,
    link_technosphere_based_on_name_unit_location,
    link_technosphere_by_activity_hash,
    normalize_units,
    strip_biosphere_exc_locations,
)
from ..utils import activity_hash
from .base import ImportBase


class LCIImporter(ImportBase):
    """Base class for format-specific importers.

    Defines workflow for applying strategies.

    Takes a database name (string) as initialization parameter.

    """

    format = "Generic LCIImporter"
    project_parameters = None
    database_parameters = None
    metadata = {}

    def __init__(self, db_name):
        self.db_name = db_name
        self.strategies = [
            normalize_units,
            drop_unspecified_subcategories,
            assign_only_product_as_production,
            strip_biosphere_exc_locations,
        ]

    @property
    def all_linked(self):
        return self.statistics()[2] == 0

    @property
    def needs_multifunctional_database(self):
        return any(ds.get("type") == "multifunctional" for ds in self.data)

    def statistics(self, print_stats: bool = True) -> Tuple[int, int, int, int]:
        links = collections.defaultdict(int)
        num_datasets = len(self.data)
        num_exchanges = 0
        for ds in self.data:
            for exc in ds.get("exchanges", []):
                num_exchanges += 1
                if "input" in exc:
                    try:
                        links[exc['input'][0]] += 1
                    except (KeyError, IndexError):
                        pass
        num_unlinked = len(
            [
                1
                for ds in self.data
                for exc in ds.get("exchanges", [])
                if not exc.get("input")
                and not (ds.get("type") == "multifunctional" and exc.get("functional"))
            ]
        )
        num_multifunctional = sum(
            1 for ds in self.data if ds.get("type") == "multifunctional"
        )
        if print_stats:
            resolved = "\n".join(["\t\t{} ({} exchanges)".format(a, b) for b, a in sorted([(v, k) for k, v in links.items()], reverse=True)])

            unique_unlinked = collections.defaultdict(set)
            for ds in self.data:
                for exc in (e for e in ds.get("exchanges", []) if not e.get("input")):
                    unique_unlinked[exc.get("type")].add(activity_hash(exc))
            unique_unlinked = sorted(
                [(k, len(v)) for k, v in list(unique_unlinked.items())]
            )
            uu = "\n\t\t".join(
                [
                    "Type {}: {} unique unlinked exchanges".format(*o)
                    for o in unique_unlinked
                ]
            )

            if num_multifunctional:
                print(
                    f"""{num_datasets} datasets, including {num_multifunctional} multifunctional datasets
\t{num_exchanges} exchanges
\tLinks to the following databases:
{resolved}
\t{num_unlinked} unlinked exchanges ({len(unique_unlinked)} types)
\t\t{uu}"""
                )
            else:
                print(
                    f"""{num_datasets} datasets
\t{num_exchanges} exchanges
\tLinks to the following databases:
{resolved}
\t{num_unlinked} unlinked exchanges ({len(unique_unlinked)} types)
\t\t{uu}"""
                )
        return num_datasets, num_exchanges, num_unlinked, num_multifunctional

    def write_project_parameters(self, data=None, delete_existing=True):
        """Write global parameters to ``ProjectParameter`` database table.

        ``delete_existing`` controls whether new parameters will delete_existing existing parameters, or just update values. The ``name`` field is used to determine if a parameter exists.

        ``data`` should be a list of dictionaries (``self.project_parameters`` is used by default):

        .. code-block:: python

            [{
                'name': name of variable (unique),
                'amount': numeric value of variable (optional),
                'formula': formula in Python as string (optional),
                optional keys like uncertainty, etc. (no limitations)
            }]

        """
        if (data or self.project_parameters) is None:
            return
        if delete_existing:
            ProjectParameter.delete().execute()
        parameters.new_project_parameters(data or self.project_parameters)

    def write_database_parameters(
        self, activate_parameters=False, delete_existing=True
    ):
        if activate_parameters:
            if self.database_parameters is not None:
                if delete_existing:
                    DatabaseParameter.delete().where(
                        DatabaseParameter.database == self.db_name
                    ).execute()
                parameters.new_database_parameters(
                    self.database_parameters, self.db_name
                )
        elif self.database_parameters:
            self.metadata["parameters"] = self.database_parameters

    def _prepare_activity_parameters(self, data=None, delete_existing=True):
        data = self.data if data is None else data

        def supplement_activity_parameter(ds, dct):
            dct.update({"database": self.db_name, "code": ds["code"]})
            if "group" not in dct:
                dct["group"] = "{}:{}".format(dct["database"], dct["code"])
            return dct

        activity_parameters = [
            supplement_activity_parameter(ds, dct)
            for ds in data
            for dct in ds.pop("parameters", [])
        ]
        by_group = lambda x: x["group"]
        activity_parameters = sorted(activity_parameters, key=by_group)

        # Delete all parameterized exchanges because
        # all exchanges are re-written, even on
        # update, which means ids are unreliable
        # Must add exchanges again manually
        bad_groups = tuple(
            {
                o[0]
                for o in ActivityParameter.select(ActivityParameter.group)
                .where(ActivityParameter.database == self.db_name)
                .tuples()
            }
        )
        ParameterizedExchange.delete().where(
            ParameterizedExchange.group << bad_groups
        ).execute()
        if delete_existing:
            # Delete existing parameters and p. exchanges if necessary
            ActivityParameter.delete().where(
                ActivityParameter.group << bad_groups
            ).execute()
        else:
            # Delete activity parameters
            # where the group name changed
            name_changed = tuple(
                {
                    o[0]
                    for o in ActivityParameter.select(ActivityParameter.group)
                    .where(
                        ActivityParameter.database == self.db_name,
                        ActivityParameter.code
                        << tuple([m["code"] for m in activity_parameters]),
                        ~(
                            ActivityParameter.group
                            << tuple([m["group"] for m in activity_parameters])
                        ),
                    )
                    .tuples()
                }
            )
            ActivityParameter.delete().where(
                ActivityParameter.group << name_changed
            ).execute()

        return activity_parameters

    def _write_activity_parameters(self, activity_parameters):
        for group, params in itertools.groupby(
            activity_parameters, lambda x: x["group"]
        ):
            params = list(params)
            # Order is important, as `new_` modifies data
            keys = {(o["database"], o["code"]) for o in params}
            parameters.new_activity_parameters(params, group)

            for key in keys:
                parameters.add_exchanges_to_group(group, key)

    def database_class(
        self, db_name: str, requested_backend: str = "sqlite"
    ) -> ProcessedDataStore:
        from multifunctional import MultifunctionalDatabase

        if self.needs_multifunctional_database:
            return MultifunctionalDatabase(db_name)
        else:
            return Database(db_name, backend=requested_backend)

    def write_database(
        self,
        data: Optional[dict] = None,
        delete_existing: bool = True,
        backend: Optional[str] = None,
        activate_parameters: bool = False,
        db_name: Optional[str] = None,
        searchable: bool = True,
        check_typos: bool = True,
        **kwargs,
    ):
        """
        Write data to a ``Database``.

        All arguments are optional, and are normally not specified.

        ``delete_existing`` effects both the existing database (it will be emptied prior to writing if True, which is the default), and, if ``activate_parameters`` is True, existing database and activity parameters. Database parameters will only be deleted if the import data specifies a new set of database parameters (i.e. ``database_parameters`` is not ``None``) - the same is true for activity parameters. If you need finer-grained control, please use the ``DatabaseParameter``, etc. objects directly.

        Args:
            * *data* (dict, optional): The data to write to the ``Database``. Default is ``self.data``.
            * *delete_existing* (bool, default ``True``): See above.
            * *activate_parameters* (bool, default ``False``). Instead of storing parameters in ``Activity`` and other proxy objects, create ``ActivityParameter`` and other parameter objects, and evaluate all variables and formulas.
            * *backend* (string, optional): Storage backend to use when creating ``Database``. Default is the default backend.

        Returns:
            ``Database`` instance.

        """
        data = self.data if data is None else data
        db_name = db_name or self.db_name
        backend = backend or "sqlite"
        self.metadata.update(kwargs)

        if activate_parameters:
            # Comes before .write_database because we
            # need to remove `parameters` key
            activity_parameters = self._prepare_activity_parameters(
                data, delete_existing
            )

        if {o["database"] for o in data} != {db_name}:
            error = "Activity database must be {}, but {} was also found".format(
                db_name, {o["database"] for o in data}.difference({db_name})
            )
            raise WrongDatabase(error)
        if len({o["code"] for o in data}) < len(data):
            seen, duplicates = set(), []
            for o in data:
                if o["code"] in seen:
                    duplicates.append(o["name"])
                else:
                    seen.add(o["code"])
            error = "The following activities have non-unique codes: {}"
            raise NonuniqueCode(error.format(duplicates))

        data = {(ds["database"], ds["code"]): ds for ds in data}

        if db_name in databases:
            # TODO: Raise error if unlinked exchanges?
            db = self.database_class(db_name)
            if delete_existing:
                existing = {}
            else:
                existing = db.load(as_dict=True)
        else:
            existing = {}
            if "format" not in self.metadata:
                self.metadata["format"] = self.format
            if (
                self.needs_multifunctional_database
                and "default_allocation" not in self.metadata
            ):
                self.metadata["default_allocation"] = "manual_allocation"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                db = self.database_class(db_name)
                db.register(**self.metadata)

        self.write_database_parameters(activate_parameters, delete_existing)

        existing.update(data)
        db.write(existing, searchable=searchable, check_typos=check_typos)

        if activate_parameters:
            self._write_activity_parameters(activity_parameters)

        print("Created database: {}".format(db_name))
        return db

    def write_excel(self, only_unlinked=False, only_names=False):
        """Write database information to a spreadsheet.

        If ``only_unlinked``, then only write unlinked exchanges.

        If ``only_names``, then write only activity names, no exchange data.

        Returns the filepath to the spreadsheet file.

        """
        fp = write_lci_matching(self.data, self.db_name, only_unlinked, only_names)
        print("Wrote matching file to:\n{}".format(fp))
        return fp

    def match_database(
        self,
        db_name=None,
        fields=None,
        ignore_categories=False,
        relink=False,
        kind=None,
    ):
        """Match current database against itself or another database.

        If ``db_name`` is None, match against current data. Otherwise, ``db_name`` should be the name of an existing ``Database``.

        ``fields`` is a list of fields to use for matching. Field values are case-insensitive, but otherwise must match exactly for a link to be valid. If ``fields`` is ``None``, use the default fields of 'name', 'categories', 'unit', 'reference product', and 'location'.

        If ``ignore_categories``, link based only on name, unit and location. ``ignore_categories`` conflicts with ``fields``.

        If ``relink``, relink exchanges even if a link is already present.

        ``kind`` can be a string or a list of strings. Common values are "technosphere", "biosphere", "production", and "substitution".

        Nothing is returned, but ``self.data`` is changed.

        """
        kwargs = {
            "fields": fields,
            "kind": kind,
            "relink": relink,
        }
        if fields and ignore_categories:
            raise ValueError("Choose between `fields` and `ignore_categories`")
        if ignore_categories:
            kwargs["fields"] = {"name", "unit", "location"}
        if db_name:
            if db_name not in databases:
                raise StrategyError("Can't find external database {}".format(db_name))
            kwargs["other"] = Database(db_name)
        else:
            kwargs["internal"] = True

        self.apply_strategy(functools.partial(link_iterable_by_fields, **kwargs))

    def create_new_biosphere(self, biosphere_name: str):
        """Create new biosphere database from unlinked biosphere flows in ``self.data``"""
        if biosphere_name in databases:
            raise ValueError(f"{biosphere_name} database already exists")

        def reformat(exc):
            return exc | {
                "type": labels.biosphere_node_default,
                "exchanges": [],
                "database": biosphere_name,
                "code": activity_hash(exc),
            }

        bio_data = {
            (flow["database"], flow["code"]): flow
            for flow in [
                reformat(exc)
                for ds in self.data
                for exc in ds.get("exchanges", [])
                if exc["type"] in labels.biosphere_edge_types and not exc.get("input")
            ]
        }

        if not bio_data:
            print(
                "Skipping biosphere database creation as all biosphere flows are linked"
            )
            return

        print(
            f"Creating new biosphere database {biosphere_name} with {len(bio_data)} flows"
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            new_bio = Database(biosphere_name)
            new_bio.register(
                format=self.format,
                comment=f"Database for unlinked biosphere flows from {self.db_name}",
            )

        new_bio.write(bio_data)
        self.apply_strategies(
            [
                functools.partial(
                    link_iterable_by_fields,
                    other=list(bio_data.values()),
                ),
            ]
        )

    def add_unlinked_flows_to_biosphere_database(
        self, biosphere_name=None, fields={"name", "unit", "categories"}
    ):
        biosphere_name = biosphere_name or config.biosphere
        assert biosphere_name in databases, "{} biosphere database not found".format(
            biosphere_name
        )

        bio = Database(biosphere_name)

        def reformat(exc):
            dct = {key: value for key, value in list(exc.items()) if key in fields}
            dct.update(
                type="emission",
                exchanges=[],
                code=activity_hash(dct),
                database=biosphere_name,
            )
            return dct

        new_data = [
            reformat(exc)
            for ds in self.data
            for exc in ds.get("exchanges", [])
            if exc["type"] == "biosphere" and not exc.get("input")
        ]

        data = bio.load()
        # Dictionary eliminate duplicates
        data.update({(biosphere_name, activity_hash(exc)): exc for exc in new_data})
        bio.write(data)

        self.apply_strategy(
            functools.partial(
                link_iterable_by_fields,
                other=(
                    obj
                    for obj in Database(biosphere_name)
                    if obj.get("type") == "emission"
                ),
                kind="biosphere",
            ),
        )

    def migrate(self, migration_name):
        if migration_name not in migrations:
            warnings.warn(
                "Skipping migration {} because it isn't installed.".format(
                    migration_name
                )
            )
        else:
            self._migrate_datasets(migration_name)
            self._migrate_exchanges(migration_name)

    def drop_unlinked(self, i_am_reckless=False):
        if not i_am_reckless:
            warnings.warn(
                "This is the nuclear weapon of linking, and should only be used in extreme cases. Must be called with the keyword argument ``i_am_reckless=True``!"
            )
        else:
            self.apply_strategies([drop_unlinked])

    def add_unlinked_activities(self):
        """Add technosphere flows to ``self.data``."""
        if not hasattr(self, "db_name"):
            raise AttributeError("Must have valid ``db_name`` attribute")
        ACTIVITY_KEYS = {"location", "comment", "name", "unit", "categories"}
        new_activities = [
            {
                k: v
                for k, v in list(obj.items())
                if obj.get("type") == "technosphere" and k in ACTIVITY_KEYS
            }
            for obj in self.unlinked
        ]
        for act in new_activities:
            act["type"] = "process"
            act["code"] = activity_hash(act)
            act["database"] = self.db_name
        self.data.extend(new_activities)
        self.apply_strategy(functools.partial(link_iterable_by_fields, other=self.data))
