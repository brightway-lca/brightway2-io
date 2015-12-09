Brightway2 input and output
===========================

.. warning:: brightway2-io is under heavy development, and is not yet ready for you* to use (`unless you're Dutch <https://www.python.org/dev/peps/pep-0020/>`__).

.. note:: You must create the core migrations files using ``bw2io.create_core_migrations()`` before doing anything else!

This package provides tools for the management of inventory databases and impact assessment methods. It is part of the `Brightway2 LCA framework <http://brightwaylca.org>`_. `Online documentation <https://brightway2.readthedocs.org/en/latest/>`_ is available, and the source code is hosted on `Bitbucket <https://bitbucket.org/cmutel/brightway2-io>`_.

In contrast with previous IO functionality in Brightway2, brightway2-io uses an iterative approach to importing and linking data. First, data is *extracted* into a common format. Next, a series of *strategies* is employed to uniquely identify each dataset and link datasets internally and to the biosphere. Following internal linking, linking to other background datasets can be performed. Finally, database data is written to disk.

This approach offers a number of benefits that help mitigate some of the serious problems in existing inventory data formats: the number of unlinked exchanges can be easily seen, linking strategies can be iteratively applied, and intermediate results can be saved.

Here is a typical usage:

.. code-block:: python

    In [1]: from bw2io import *

    In [2]: so = SingleOutputEcospold2Importer("/path/to/ecoinvent/3.1/cutoff/datasets", "ecoinvent 3.1 cutoff")
    11301/11301 (100%) |||||||||||||||||||||||||||||||||||||||||||||||||||||||||| Time: 0:01:56
    Converting to unicode
    Extracted 11301 datasets in 262.63 seconds

    In [3]: so.apply_strategies()
    Applying strategy: remove_zero_amount_coproducts
    Applying strategy: remove_zero_amount_inputs_with_no_activity
    Applying strategy: es2_assign_only_product_with_amount_as_reference_product
    Applying strategy: assign_single_product_as_activity
    Applying strategy: create_composite_code
    Applying strategy: link_biosphere_by_flow_uuid
    Applying strategy: link_internal_technosphere_by_composite_code
    Applying strategy: delete_exchanges_missing_activity
    Applying strategy: delete_ghost_exchanges
    Applying strategy: mark_unlinked_exchanges

    In [4]: so.statistics()
    11301 datasets
    521712 exchanges
    0 unlinked exchanges
    Out[4]: (11301, 521712, 0)

    In [5]: so.write_database()

Note that brightway2-io can't magically make problems in databases go away.

Brightway2-io provides the following importers:

    * Ecospold 1 (single & multioutput)
    * Ecospold 1 impact assessment
    * Ecospold 2
    * SimaPro CSV (single & multioutput)
    * SimaPro CSV impact assessment

As well as the following exporters:

    * Excel
    * Gephi GEXF
    * Matlab

Additionally, data can be imported or exported into Brightway packages, and the entire data directory can be snapshotted.

Importing an LCI database
=========================

LCI database can be imported from ecospold 1 (both single- and multioutput), ecospold 2, and SimaPro CSV (single- and multioutput). Multioutput datasets are allocated to single-output datasets.

Importing from ecospold 1
-------------------------

Importing from ecospold 1 is relatively simple. Multioutput products are allocated to single output products using the given allocation factors using the strategy ``es1_allocate_multioutput``. The reference product is then assigned using the strategy ``assign_only_product_as_production``.

Next, some basic data cleanup is performed. Integer codes are removed, as these are not used consistently by different LCA software (``clean_integer_codes``). Unspecified subcategories are removed (i.e. ``('air', 'unspecified')`` is changed to ``('air',)``) using ``drop_unspecified_subcategories``. Biosphere exchange names and categories are normalized using ``normalize_biosphere_categories`` and ``normalize_biosphere_names``. Biosphere exchanges are removed, as biosphere flows do not have locations (``strip_biosphere_exc_locations``).

Next, a unique activity code is generated for each dataset, using a combination of the name, categories, location, and unit (``set_code_by_activity_hash``).

Finally, biosphere flows are linked to the default biosphere database, and internal technosphere flows are linked using ``link_technosphere_by_activity_hash``.

Importing from ecospold 2
-------------------------

Importing from ecospold 2 is a bit complex, because although ecospold 2 gives unique IDs for many fields, which helps in linking, the current implementation has some `known issues <http://www.ecoinvent.org/database/ecoinvent-version-3/ecoinvent-v30/known-data-issues/>`__ which have to be resolved or ignored by the importer.

.. warning:: Brightway2 cannot reproduce the LCI and LCIA results given by the ecoinvent centre. The technosphere matrix used by ecoinvent cannot be reproduced from the provided unit process datasets. However, the differences for most products are quite small.

We start by removing some exchanges from most datasets. Specifically, we remove exchanges with amounts of zero, both coproducts and technosphere or biosphere inputs (``remove_zero_amount_coproducts`` and ``remove_zero_amount_inputs_with_no_activity``).

We then assign reference products. Although each unit process should have a single output, coproducts which have been allcoated away are often still included, with amounts of zero. We use two strategies to choose the reference product: ``es2_assign_only_product_with_amount_as_reference_product`` and ``assign_only_product_as_production``.

Next, a composite code is generated, using the UUID of the activity and the product (``create_composite_code``).

Biosphere flow exchanges are now normalized (``drop_unspecified_subcategories``) and linked (``link_biosphere_by_flow_uuid``). Internal technosphere exchanges are also linked, using the composite codes (``link_internal_technosphere_by_composite_code``).

Not all technosphere exchanges are linked, however. We need to drop two different types of exchanges, as we have no way of linking them. First, there are some exchanges with listed products but no listed activities - and no activity in the database produces these products. Removal is done with the strategy ``delete_exchanges_missing_activity``.

Additionally, there are some exchanges with listed products and activities - but the given activity doesn't produce the listed product. These exchanges also have to be deleted, using the strategy ``delete_ghost_exchanges``.

.. note:: As of March 2015, only the cutoff version completely avoids the two problems listed above.

Importing from SimaPro
----------------------

Importing SimaPro CSV files is also a bit of a headache. Pré, the makers of SimaPro, have done a lot of work to make LCA software accessible and understandable. This work includes making changes to process names and other metadata, which makes linking these processes back to original ecoinvent data difficult. Fortunately, Pré has been very helpful is supplying correspondence files, which we can use to move (to the best of our ability) from the "SimaPro world" to "ecoinvent world".

.. note:: Importing SimaPro XML export files is not recommended, as there are bugs with exporting ecoinvent 3 processes.

What to do with unmatched exchanges?
------------------------------------

If there are unlinked exchanges, you have several options. If you aren't sure what to do yet, you can save a temporary copy (that can be loaded later) using ``.write_unlinked("some name")``.

Calling ``.statistics()`` will show what kind of exchanges aren't linked, e.g.:

.. code-block:: python

    In [4]: sp.statistics()
    366 datasets
    3991 exchanges
    2639 unlinked exchanges
      Type biosphere: 170 unique unlinked exchanges
      Type technosphere: 330 unique unlinked exchanges

The options to examine or resolve the unlinked exchanges are:

    * You can write a spreadsheet of the characterization factors, including their linking status, with ``.write_excel("some name")``.
    * You can apply new linking strategies with ``.apply_strategies([some_new_strategy])``. Note that this method requires a *list* of strategies.
    * You can match technosphere or biosphere exchanges to other background databases using ``.match_database("another database")``.
    * TODO: Add unlinked tech processes to current database
    * To resolve unlinked biosphere exchanges which simply don't exist in your current biosphere database, you can:

        * Add them to the biosphere database with ``add_unlinked_flows_to_biosphere_database()``
        * Create a new biosphere database with ``create_new_biosphere("new biosphere name")``
        * Add the biosphere flows to the database you are currently working on (LCI databases can include both process and biosphere flows) with TODO: ``add_unlinked_biosphere_flows_to_current_database()``

.. note:: These methods have several options, and you should understand what they do and read their documentation before choosing between them.

.. note:: You can't write an LCI database with unlinked exchanges.

Migrations
==========

Sometimes the only way to correctly link activities or biosphere flows is by applying a list of name (or other field) transforms. For example, SimaPro will export a process named "[sulfonyl]urea-compound {RoW}| production | Alloc Rec, S", which corresponds to the ecoinvent process "[sulfonyl]urea-compound production", with reference product "[sulfonyl]urea-compound" and location "RoW". In another example, in ecoinvent 2, emissions of water to air were measured in kilograms, and in ecoinvent 3, emissions of water to air are measured in cubic meters. In this case, our migration would look like this:

.. code-block:: python

    {
        'fields': ['name', 'categories', 'type', 'unit'],
        'data': [
            (
                # First element is input data in the order of `fields` above
                ('Water', ('air',), 'biosphere', 'kilogram'),
                # Second element is new values to substitute
                {
                    'unit': 'cubic meter',
                    'multiplier': 0.001
                }
            )
        }
    }

We call the application of transform lists "migrations", and they are applied with the ``.migrate(migrations_name)`` method.

TODO: Because migrations can be tricky, a log file is kept for each migration, and should be examined.

If the numeric values in an exchange need to changed, the special key 'multiplier' is used, where new_amount = multiplier * old_amount. Uncertainty information and formulas are adjusted automatically, if possible (see ``utils.rescale_exchange``).

A few additional notes:

* Migrations change the underlying data, but do not do any linking - you will also have to apply linking strategies after a migration.
* Migrations can specify any number of fields, but of course the fields must be present in the importing database.
* TODO: Migrations can be specified in an excel template. Template files must be processed using ``convert_migration_file``.
* Subcategories are not expanded automatically, so a separate row in the migrations file would be needed for e.g. ``water (air, non-urban air or from high stacks)``.

Importing an LCIA method
========================

LCIA methods can be imported from ecospold 1 XML files (``EcoinventLCIAImporter``) and SimaPro CSV files (``SimaProLCIACSVImporter``).

When importing an LCIA method or set of LCIA methods, you should specify the biosphere database to link against e.g. ``EcoinventLCIAImporter("some file path", "some biosphere database name")``. If no biosphere database name is provided, the default ``biosphere3`` database is used.

Both importers will attempt to normalize biosphere flow names and categories to the ecospold2 standard, using the strategies:

    * ``normalize_simapro_lcia_biosphere_categories``
    * ``normalize_simapro_biosphere_names``
    * ``normalize_biosphere_names``
    * ``normalize_biosphere_categories``

Next, the characterization factors are examined to see if they are only given for root categories, e.g. ``('air',)`` and not ``('air', 'urban air close to ground')``. If only root categories are characterized, then we assume that the characterization factors also apply to all subcategories, using the strategy  ``match_subcategories``.

Finally, linking to the given or default biosphere database is attempted, using the strategy ``link_iterable_by_fields`` and the standard fields: name, categories, unit, location. Note that biosphere flows do not actually have a location.

You can now check the linking statistics. If all biosphere flows are linked, write the LCIA methods with ``.write_methods()``. Note that attempting to write an existing method will raise a ``ValueError`` unless you use ``.write_methods(overwrite=True)``, and trying to write methods which aren't completely linked will also raise a ``ValueError``.

If there are unlinked characterization factors, you have several options. If you aren't sure what to do yet, you can save a temporary copy (that can be loaded later) using ``.write_unlinked("some name")``. The options to examine or resolve the unlinked characterization factors are:

    * You can write a spreadsheet of the characterization factors, including their linking status, with ``.write_excel("some name")``.
    * You can apply new linking strategies with ``.apply_strategies([some_new_strategy])``. Note that this method requires a *list* of strategies.
    * TODO: You can write all biosphere flows to a new biosphere database with ``.create_new_biosphere("some name")``.
    * If you are satisfied that you don't care about the unlinked characterization factors, you can drop them with ``.drop_unlinked()``.
    * Alternatively, you can add the missing biosphere flows to the biosphere database using ``.add_missing_cfs()``.

Testing
=======

Tests should (eventually) have 100% coverage, with most effort going to testing edge cases for strategies, and for importing real-world databases.

Tests are run using `nose <https://nose.readthedocs.org/en/latest/>`__.

To run tests in parallel:

    nosetests --processes=<num_cpus_desired> --process-timeout=20

To generate a test coverage report:

    nosetests --with-coverage --cover-html --cover-package=bw2io

TODO
====

    * Tests for each strategy
    * New migrations module

        - ecoinvent 2.2 > 3.01 (each system model)
        - ecoinvent 3.01 > 3.1 (each system model)
        - SimaPro > ecoinvent biosphere

    * US LCI importer

        - Add DUMMY processes (strategy to add unlinked activities)
        - Fix names

            + Easy way to get missing and matching values in new version?

    * SimaPro CSV: Can uncertainty values be specific if amount is a formula? What would that mean?
    * SimaPro CSV: Extract and apply unit conversions

    * Comparison chart of all freely available databases

        - USDA
        - US LCI
        - GreenDelta nexus website

    * Specific issues

        - SimaPro LCIA importer - waste types seem incorrect
        - Ned to find a clever way to replace formula names that conflict with Python keywords
