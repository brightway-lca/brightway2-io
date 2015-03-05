Brightway2 input and output
===========================

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

Importing from ecospold 2
-------------------------

Importing from SimaPro
----------------------

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

TODO
====

    * Tests for each strategy
    * Documentation for each strategy
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

    * Fix excel and matlab output (search tech_dict and reverse_dict) for bw2calc 1.0
