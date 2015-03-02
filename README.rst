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
    * SimaPro CSV
    * SimaPro CSV impact assessment

As well as the following exporters:

    * Excel
    * Gephi GEXF
    * Matlab

LCI databases and LCIA methods which have not been completed linked can be saved as UnlinkedData objects.

Additionally, data can be imported or exported into Brightway packages, and the entire data directory can be snapshotted.

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

    * biosphere3 should be an importer, with
        - strategy - set type
        - strategy - drop unspecified subcategory
        - strategy - create root level flows (no subcategory) with consistent UUIDs

    * Add LCIA XML files and excel updates
    * Split data directory into inv and lcia subdirectories
