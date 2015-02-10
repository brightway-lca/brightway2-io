Brightway2 input and output
===========================

This package provides tools for the management of inventory databases and impact assessment methods. It is part of the `Brightway2 LCA framework <http://brightwaylca.org>`_. `Online documentation <https://brightway2.readthedocs.org/en/latest/>`_ is available, and the source code is hosted on `Bitbucket <https://bitbucket.org/cmutel/brightway2-io>`_.

In contrast with previous IO functionality in Brightway2, brightway2-io uses an iterative approach to importing and linking data. First, data is *extracted* into a common format. Next, a series of *strategies* is employed to uniquely identify each dataset and link datasets internally and to the biosphere. Following internal linking, linking to other background datasets can be performed. Finally, database data is written to disk.

This approach offers a number of benefits that help mitigate some of the serious problems in existing inventory data formats: the number of unlinked exchanges can be easily seen, linking strategies can be iteratively applied, and intermediate results can be saved.

Here is a typical usage:

.. code-block:: python

    In [1]: from bw2io import *

    In [2]: so = SingleOutputEcospold2Importer("/path/to/ecoinvent/3.1/default/datasets", "ecoinvent 3.1 apos")
    11329/11329 (100%) ||||||||||||||||||||||||||||||||||||||||||||||||| Time: 0:01:14
    Converting to unicode
    Extracted 11329 datasets in 195.89 seconds

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
    116 exchanges couldn't be linked and were deleted. See the logfile for details:
        /Users/cmutel/brightway2dev/logs/Ecospold2-import-error.AKFRQy.log
    Applying strategy: mark_unlinked_exchanges

    In [4]: so.write_database()

Note that brightway2-io can't magically make problems in databases go away.

Brightway2-io provides the following importers:

    * Ecospold 1 (single & multioutput)
    * Ecospold 1 impact assessment
    * Ecospold 2
    * SimaPro CSV

As well as the following exporters:

    * Excel
    * Gephi GEXF
    * Matlab

Additionally, data can be imported or exported into Brightway packages, and the entire data directory can be snapshotted.

TODO:

    * Ecospold 1: Link to ecoinvent 2 background databases.
    * Ecospld 1 multioutput: Clean ``code`` field.
    * SimaPro CSV LCIA importer.
    * SimaPro CSV: Link to ecoinvent 2 (with detoxify) background databases.
    * What biosphere flows does SimaPro 8 use? Ecoinvent 2 or 3?
    * Tests.
