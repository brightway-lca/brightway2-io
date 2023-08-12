Brightway2 input and output
===========================

.. image:: https://img.shields.io/pypi/v/bw2io.svg
   :target: https://pypi.org/project/bw2io/
   :alt: bw2io pypi version
   
.. image:: https://img.shields.io/conda/vn/conda-forge/bw2io.svg
   :target: https://anaconda.org/conda-forge/bw2io
   :alt: bw2io conda-forge version

.. image:: https://ci.appveyor.com/api/projects/status/7dox9te430eb2f8h?svg=true
   :target: https://ci.appveyor.com/project/cmutel/brightway2-io
   :alt: bw2io appveyor build status

.. image:: https://coveralls.io/repos/bitbucket/cmutel/brightway2-io/badge.svg?branch=master
    :target: https://coveralls.io/bitbucket/cmutel/brightway2-io?branch=default
    :alt: Test coverage report

This package provides tools for the import, export, and management of inventory databases and impact assessment methods. It is part of the `Brightway2 LCA framework <https://brightway.dev/>`_. `Online documentation <https://2.docs.brightway.dev/>`_ is available, and the source code is hosted on `Github <https://github.com/brightway-lca/brightway2-io>`_.

Compatibility with Brightway2X
------------------------------
*Until there is a solution to using seamlessly bw2io to use any flow list (to create the biosphere) regardless of the bw2io and brightway2X package version, specific versions of bw2io must be used for compatibility with specific ecoinvent versions.*

This code in this repository is used to release packages for both `Brightway25 <https://github.com/brightway-lca/brightway25>`_ (the current default branch of this repository `"master" <https://github.com/brightway-lca/brightway2-io/tree/master>`_) and `Brightway2 <https://github.com/brightway-lca/brightway2>`_ (the `"legacy" <https://github.com/brightway-lca/brightway2-io/tree/legacy>`_ branch in this repository).
 
- For the *Brightway2 compatible* version, use a ``0.8.X`` version.
   - use version ``0.8.7`` to have a Brightway2 installation compatible with Ecoinvent 3.8
     
     If you have a working environment with brightway2 ::     
   
         conda install -c conda-forge -c cmutel bw2io=0.8.7 
      
     If you have are starting a new environment ::
      
          conda install -c conda-forge -c cmutel brightway2 bw2io=0.8.7 
      
          
   - use version ``0.8.8`` to have a Brightway2 installation compatible with Ecoinvent 3.9 ::
   
      conda install -c conda-forge bw2io=0.8.8 # If you have a working environment with brightway2
      conda install -c conda-forge brightway2 bw2io=0.8.8 # If you are starting a new environment
      
      
      
- For the *Brightway25 compatible* version, use a ``0.9.X`` version.
   - use version ``0.9.dev11`` to have a Brightway2 installation compatible with Ecoinvent 3.8.
   
     *This version is only available through pypi.*
     
     If you already have a brightway25 environment, do ::
     
     
         pip install bw2io==0.9.dev11
         
   - use version ``0.9.dev10`` to have a Brightway2 installation compatible with Ecoinvent 3.9 ::
   
          conda install -c conda-forge -c cmutel bw2io=0.9.dev10 # If you have a working environment with brightway25
          conda install -c conda-forge -c cmutel brightway25 bw2io=0.9.dev10 # If you are starting a new environment
     
   
TL;DR
^^^^^

Summing up, you need a specific version of bw2io for specific versions of ecoinvent 3.8 and 3.9.::

    +------------------|-------------------|------------------+ 
    |Ecoinvent version | Brightway Version | bw2io Version    | 
    +==================|===================|==================+ 
    |ecoinvent 3.8     | bw2               | bw2io 0.8.7      | 
    +------------------|-------------------|------------------+ 
    |ecoinvent 3.9     | bw2               | bw2io 0.8.8      | 
    +------------------|-------------------|------------------+ 
    |ecoinvent 3.8     | bw25              | bw2io 0.9.dev11  | 
    +------------------|-------------------|------------------+ 
    |ecoinvent 3.9     | bw25              | bw2io 0.9.dev10  | 
    +------------------|-------------------|------------------+ 



Bw2io approach
---------------

In contrast with previous IO functionality in Brightway2, brightway2-io uses an iterative approach to importing and linking data. First, data is *extracted* into a common format. Next, a series of *strategies* is employed to uniquely identify each dataset and link datasets internally and to the biosphere. Following internal linking, linking to other background datasets can be performed. Finally, database data is written to disk.

This approach offers a number of benefits that help mitigate some of the serious problems in existing inventory data formats: the number of unlinked exchanges can be easily seen, linking strategies can be iteratively applied, and intermediate results can be saved.

Here is a typical usage:


.. code-block:: python

   In [1]: import bw2io as bi

   In [2]: import brightway2 as bw2

   In [3]: bi.__version__
   Out[3]: (0, 8, 7)

   In [4]: bw2.__version__
   Out[4]: (2, 4, 1)

   In [5]: importer = bi.SingleOutputEcospold2Importer('path/to/ecoinvent/datasets/', 'ei_38_cutoff')
   Extracting XML data from 19565 datasets
   Extracted 19565 datasets in 19.21 seconds

   In [6]: importer.apply_strategies()
   Applying strategy: normalize_units
   Applying strategy: update_ecoinvent_locations
   Applying strategy: remove_zero_amount_coproducts
   Applying strategy: remove_zero_amount_inputs_with_no_activity
   Applying strategy: remove_unnamed_parameters
   Applying strategy: es2_assign_only_product_with_amount_as_reference_product
   Applying strategy: assign_single_product_as_activity
   Applying strategy: create_composite_code
   Applying strategy: drop_unspecified_subcategories
   Applying strategy: fix_ecoinvent_flows_pre35
   Applying strategy: drop_temporary_outdated_biosphere_flows
   Applying strategy: link_biosphere_by_flow_uuid
   Applying strategy: link_internal_technosphere_by_composite_code
   Applying strategy: delete_exchanges_missing_activity
   Applying strategy: delete_ghost_exchanges
   Applying strategy: remove_uncertainty_from_negative_loss_exchanges
   Applying strategy: fix_unreasonably_high_lognormal_uncertainties
   Applying strategy: set_lognormal_loc_value
   Applying strategy: convert_activity_parameters_to_list
   Applying strategy: add_cpc_classification_from_single_reference_product
   Applying strategy: delete_none_synonyms
   Applied 21 strategies in 3.62 seconds

   In [7]: importer.statistics()
   19565 datasets
   629959 exchanges
   0 unlinked exchanges

   Out[7]: (19565, 629959, 0)

   In [8]: if importer.statistics()[2] == 0:
   ...:     importer.write_database()
   ...: else:
   ...:     print("There are unlinked exchanges.")
   ...:     importer.write_excel()
   ...: 
   19565 datasets
   629959 exchanges
   0 unlinked exchanges

   Writing activities to SQLite3 database:
   0% [##############################] 100% | ETA: 00:00:00
   Total time elapsed: 00:02:29
   Title: Writing activities to SQLite3 database:
   Started: 11/07/2022 11:55:57
   Finished: 11/07/2022 11:58:26
   Total time elapsed: 00:02:29
   CPU %: 32.90
   Memory %: 11.17
   Created database: ei_38_cutoff


Note that brightway2-io can't magically make problems in databases go away.
