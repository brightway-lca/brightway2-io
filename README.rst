Brightway2 input and output
===========================

This package provides tools for the management of inventory databases and impact assessment methods. It is part of the `Brightway2 LCA framework <http://brightwaylca.org>`_. `Online documentation <https://brightway2.readthedocs.org/en/latest/>`_ is available, and the source code is hosted on `Bitbucket <https://bitbucket.org/cmutel/brightway2-data>`_.

Provides the following importers:

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
* Tests.

SimaPro questions:
* SimaPro 8 and new ecoinvent biosphere flow names?
