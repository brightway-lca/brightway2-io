Brightway2 input and output
===========================

This package provides tools for the management of inventory databases and impact assessment methods. It is part of the `Brightway2 LCA framework <http://brightwaylca.org>`_. `Online documentation <https://brightway2.readthedocs.org/en/latest/>`_ is available, and the source code is hosted on `Bitbucket <https://bitbucket.org/cmutel/brightway2-data>`_.

In contrast with previous IO functionality in Brightway2, brightway2-io uses an iterative approach to importing and linking data. First, data is *extracted* into a common format. Next, a series of *strategies* is employed to uniquely identify each dataset and link datasets internally and to the biosphere. Following internal linking, linking to other background datasets can be performed. Finally, database data is written to disk.

This approach offers a number of benefits that help mitigate some of the serious problems in existing inventory data formats: the number of unlinked exchanges can be easily seen, linking strategies can be iteratively applied, and intermediate results can be saved.

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
    * SimaPro CSV: Link to ecoinvent 2 (with detoxify) background databases.
    * What biosphere flows does SimaPro 8 use? Ecoinvent 2 or 3?
    * Tests.
