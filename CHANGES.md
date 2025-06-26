# `bw2io` Changelog

## 0.9.11 (2025-06-23)

* [#316: Add ability to separate ecoSpold2 products and processes when importing](https://github.com/brightway-lca/brightway2-io/pull/316)

## 0.9.10 (2025-06-23)

* [#288: Remove version pin to Numpy `<2`](https://github.com/brightway-lca/brightway2-io/issues/288)

## 0.9.9 (2025-04-10)

* Add `use_mp` flag to `import_ecoinvent_release`

## 0.9.8 (2025-03-27)

* Append to existing LCIA impact categories (`methods`) during `import_ecoinvent_release` instead of overwriting

## 0.9.7 (2025-03-21)

* Fix project switch when restoring from older archives

## 0.9.6 (2025-02-01)

* Compatibility fix for project archives produced pre-`0.9.5`.

## 0.9.5 (2025-01-14)

* [#297: Don't rescale `minimum` and `maximum` twice.](https://github.com/brightway-lca/brightway2-io/pull/297)
* [#295: Include project metadata on backup/restore](https://github.com/brightway-lca/brightway2-io/pull/295)
* [#294: Fix Extraction of `CPC` & `HS2017` classifications](https://github.com/brightway-lca/brightway2-io/pull/294)
* [#292: Fix multiline comment extraction from ecospold2](https://github.com/brightway-lca/brightway2-io/pull/292)
* [#264: Add custom regex functionality to `split_simapro_name_geo`](https://github.com/brightway-lca/brightway2-io/pull/264)

## 0.9.4 (2024-12-05)

* Making name shortening in MFP name generation configurable in `SimaProBlockCSVImporter`

## 0.9.3 (2024-12-05)

* Add ability to use in-memory `Randonneur` datapackages, e.g. loaded from the Excel template

## 0.9.2 (2024-12-02)

* Remove `multifunctional` and `bw_simapro_csv` as dependencies - add install variant `bw2io[multifunctional]`

## 0.9.1 (2024-12-02)

* Fix product information ecospold2 extraction with invalid inputs

## 0.9 (2024-11-27)

* Allow separate products for SimaPro block CSV importer
* Add more filter options to `link_iterable_by_fields`
* Make printed importer statistics better
* [#284 Add `ecospold2` product information from `MasterData/IntermediateExchanges.xml`](https://github.com/brightway-lca/brightway2-io/issues/284)
* [#282 Exchange Extractor for Ecospold2 only extracts CPC classification](https://github.com/brightway-lca/brightway2-io/issues/282)
* [#272 Clear Parameters as part of `overwrite=True`](https://github.com/brightway-lca/brightway2-io/pulls/272)

### 0.9.DEV41 (2024-10-15)

* Also convert `market group for electricity` to `kWh`

### 0.9.DEV40 (2024-10-14)

* Add `split_simapro_name_geo_curly_brackets` strategy
* Add `remove_biosphere_location_prefix_if_flow_in_same_location` strategy

### 0.9.DEV39 (2024-10-13)

* Add `create_products_as_new_nodes` strategy
* Add additional configuration options for `randonneur`
* Update tests for recent `bw2data` changes
* Updates for vocab.sentier.dev units URL change

### 0.9.DEV38 (2024-09-12)

* Fix #274: correctly set units for namespaced methods when importing ecoinvent with ecoinvent_interface

### 0.9.DEV37 (2024-09-04)

* Fix out of order but with `create_randonneur_excel_template_for_unlinked`

### 0.9.DEV36 (2024-09-04)

* Add `create_randonneur_excel_template_for_unlinked`

### 0.9.DEV35 (2024-09-02)

* Add method to directly apply `randonneur` transformations
* Add `create_new_database_for_flows_with_missing_top_level_context` method
* Add `normalize_simapro_labels_to_brightway_standard` method
* Add `match_against_top_level_context` function
* Add `match_against_only_available_in_given_context_tree` method
* Add `create_regionalized_biosphere_proxies` method
* Allow `add_extra_attributes`

### 0.9.DEV34 (2024-08-21)

* Restore Py 3.9 compatibility

### 0.9.DEV33 (2024-08-15)

* Packaging fix

### 0.9.DEV32 (2024-08-15)

* Add `SimaProUnitConverter` which uses `https://vocab.sentier.dev/` for unit conversion and harmonization
* Improved robustness of `rescale_exchange`
* Fixed bug with SimaPro name-location strings with spaces inside

### 0.9.DEV31 (2024-08-14)

* Improved statistics reporting for importers

### 0.9.DEV30 (2024-07-23)

* Change default to namespace LCIA methods in ecoinvent imports

### 0.9.DEV29 (2024-07-09)

* Restore Python 3.9 compatibility
* Compatibility with SimaPro multifunctionality via `bw_simapro_csv` and `multifunctional`.

### 0.9.DEV28 (2024-06-21)

* Allow imports of some invalid ecospold1 files
* Fix fetching of remote data catalogues in `remote`

### 0.9.DEV27 (2024-05-07)

* [#256 Change to new packaging template](https://github.com/brightway-lca/brightway2-io/pull/256)
* [#253 Complete ecospold1 import and export](https://github.com/brightway-lca/brightway2-io/pull/253)
* [#252 Water lake and missing geodata](https://github.com/brightway-lca/brightway2-io/pull/252)
* [#251 Added missing localized water flow](https://github.com/brightway-lca/brightway2-io/pull/251)
* [#237 Add CAS numbers](https://github.com/brightway-lca/brightway2-io/pull/237)
* [#235 add flexibility to `backup_project_directory()`](https://github.com/brightway-lca/brightway2-io/pull/235)
* [#217 Missing attribute during import](https://github.com/brightway-lca/brightway2-io/pull/217)
* [#216 Ensure categories are tuple](https://github.com/brightway-lca/brightway2-io/pull/216)
* Add extractor for SimaPro LCIA 9.5 Project CSV files

### 0.9.DEV26 (2023-11-12)

* Change `import_ecoinvent_release` to allow patching existing biosphere databases

### 0.9.DEV25 (2023-11-10)

* Fix missing import from dev24
* Some documentation improvements

### 0.9.DEV24 (2023-11-09)

* [#227 Add `import_ecoinvent_release` utility](https://github.com/brightway-lca/brightway2-io/pull/227)
* [#222 Move `KEYS` variable to function header](https://github.com/brightway-lca/brightway2-io/pull/222)
* [#219 Don't apply set_`biosphere_type` twice](https://github.com/brightway-lca/brightway2-io/pull/219)

### 0.9.DEV23 (2023-09-17)

* Use bw2data for cache filepath
* Bunch of small issue fixes
* [#207 Fixed float parsing errors](https://github.com/brightway-lca/brightway2-io/pull/207)
* [#213 Reparametrize lognormals for ecospold2 imports](https://github.com/brightway-lca/brightway2-io/pull/213)

### 0.9.DEV22 (2023-09-15)

* Pinned dependencies to fix environment problems

### 0.9.DEV21 (2023-08-12)

* #138: Fixed import of SimaPro process with multiple literature refs

### 0.9.DEV20 (2023-08-12)

* #204: Fix unsupported operand
* #136: Import invalid ecospold1 XML data
* #167: field equality strategy and default location strategy
* Purge pyprind in place of tqdm

### 0.9.DEV19 (2023-06-08)

** Note: This release has be withdrawn, as has bw2data 4.0.DEV19!**

* Fix compatibility with bw2data 4.0.DEV19

### 0.9.DEV18 (2023-06-06)

* Fix compatibility with Python 3.8 and 3.9
* Fix incomplete project downloads causing corruption
* Fix directory creation on Windows

### 0.9.DEV17 (2023-04-18)

* Add capability to install remote projects for quicker starting and more flexibility

### 0.9.DEV16 (2023-04-18)

* Restore `bw2parameters` import to previous API

### 0.9.DEV15 (2023-04-07)

* Remove `psutil` dependency

### 0.9.DEV14 (2023-03-16)

* Fix stream error when reading `tar` project archive

### 0.9.DEV13 (2023-03-16)

* Update EXIOBASE biosphere correspondence for ecoinvent 3.9

### 0.9.DEV12 (2023-03-15)

* [PR 163](https://github.com/brightway-lca/brightway2-io/pull/163): Update ecoinvent_lcia and ecospold1 files to NumPy Docstring standard
* [PR 161](https://github.com/brightway-lca/brightway2-io/pull/161): forward-port of [#160](https://github.com/brightway-lca/brightway2-io/pull/160)
* [PR 157](https://github.com/brightway-lca/brightway2-io/pull/157): Support `10^` as `10E` in Simapro CSV imports
* [PR 150](https://github.com/brightway-lca/brightway2-io/pull/150): Add pypi and conda-forge badge and update install instructions
* [PR 147](https://github.com/brightway-lca/brightway2-io/pull/147): Raise error when empty directory given to ecospold2 importer
* [PR 142](https://github.com/brightway-lca/brightway2-io/pull/142): CVE-2007-4559 Patch
* Add `collapse_products` and `prune` flags to `useeio11`

### 0.9.DEV10 (2022-10-13)

* Ecoinvent 3.9 compatibility
* Make extractor switchable in `SimaProCSVImporter`

### 0.9.DEV9 (2022-06-19)

* Change to shift all variable names to uppercase instead of lowercase from SimaPro CSV files. This helps avoid most builtin symbols.
* Improve performance of SimaPro CSV variable mangling by compiling regular expressions

### 0.9.DEV8 (2022-06-02)

* Merge [#119](https://github.com/brightway-lca/brightway2-io/pull/119): Fix variable error in ecospold1 extraction
* Fix [#124: Custom output dir for Excel/CSV export](https://github.com/brightway-lca/brightway2-io/issues/124)
* Fix [#131: Issue when importing CSV inventories](https://github.com/brightway-lca/brightway2-io/issues/131)
* Normalize field `reference unit` during unit normalization
* JSON-LD LCIA importer: Can't assume some fields are present

## 0.9.DEV7 (2022-01-11)

* Change label for chemical formulas in ecospold2 import from `formula` to `chemical formula`
* Add mathematical formula field `mathematicalRelation` from ecospold2 imports as `formula`
* Add variables names for exchanges and exchange properties in ecospold2 imports
* Add strategy to lookup chemical synonyms in [ChemIDPlus](https://chem.nlm.nih.gov/chemidplus/)

## 0.9.DEV6 (2021-10-22)

* Don't export `id` field in Excel/CSV

## 0.9.DEV4 (2021-10-20)

* Fix bug in Ecospold 1 LCIA Importer

## 0.9.DEV4 (2021-10-14)

* Continued work on JSON-LD imports
* JSON-LD LCIA importer
* Shortcut to import US EEIO 1.1 database and LCIA methods

## 0.9.DEV3 (2021-10-01)

* Partial support for JSON-LD imports

## 0.9.DEV2 (2021-09-29)

* Compatibility with ecoinvent 3.9 LCI & LCIA

## 0.9.DEV1

## Breaking changes

### Python 2 compatibility removed

Removing the Python 2 compatibility layer allows for much cleaner and more compact code, and the use of some components from the in-development Brightway version 3 libraries.

## Background changes

### Use of `bw_processing`

We now use [bw_processing](https://github.com/brightway-lca/bw_processing) to create processed arrays and magic constants.

## Smaller changes

* Merged [PR #81](https://github.com/brightway-lca/brightway2-io/pull/81), SimaPro mappings for ecoinvent 3.4. Thanks @PascalLesage
* Merged [PR #80](https://github.com/brightway-lca/brightway2-io/pull/80), SimaPro mappings for ecoinvent 3.5. Thanks @PascalLesage
* Merged [PR #82](https://github.com/brightway-lca/brightway2-io/pull/82), fix ecoinvent versus SimaPro different signs of waste treatment processes. Thanks @PascalLesage

### 0.8.7 (2021-10-14)

* Fix bug in Ecospold 1 LCIA Importer

### 0.8.6 (2021-09-29)

* Merge [PR 101]() to improve Excel reading speeds
* Patch and use Ecoinvent 3.8 LCIA implementation

### 0.8.5 (2021-09-21)

* Update to ecoinvent 3.8 flows

### 0.8.4 (2021-07-13)

* Add `split_exchanges` strategy

#### 0.8.3.1 (2021-03-10)

* Expose update functions in package namespace

### 0.8.3 (2021-03-10)

* Update to ecoinvent 3.7 flows

### 0.8.2 (2021-02-25)

* Fix openpyxl extraction giving formulas instead of numerical values

### 0.8.1 (2021-02-25)

* Fix [#83](https://github.com/brightway-lca/brightway2-io/issues/83): Skip data that can't be exported to Excel.
* Fix [#85](https://github.com/brightway-lca/brightway2-io/issues/85): Inconsistent `categories` types in base data

## 0.8.0 (2021-02-23)

* Switch from xlrd to [openpyxl](https://openpyxl.readthedocs.io/en/stable/) for reading `.xlsx` files.
* Added Excel and CSV importer for LCIA methods
* Merged [PR #77](https://github.com/brightway-lca/brightway2-io/pull/77): Add synonyms when importing ecospold2 files. Thanks @BenPortner.
* Merged [PR #76](https://github.com/brightway-lca/brightway2-io/pull/76): Correctly import reference products from Excel. Thanks @BenPortner.
* Import exchange properties from ecospold2 files.
* `bw2io.extractors.excel.ExcelExtractor` now properly handles internal Excel errors.

### 0.7.13

* Switch to openpyxl for xlsx imports
* Port Ben Portner's fixes for CSV encoding and newline handling
* Add extraction of ecospold2 exchange properties
* PR [#72](https://github.com/brightway-lca/brightway2-io/pull/72): expose `objs` argument in `write_lci_csv`
* Handle Excel error values correctly when extracting

### 0.7.12.1 (2020-03-12)

Fix bug in importing sample database (missing files)

### 0.7.12 (2020-02-25)

Add CAS number to default biosphere flows

### 0.7.11.3 (2019-10-31)

Fix problem in EXIOBASE import where elements were all set on the diagonal

### 0.7.11.2 (2019-10-30)

Improve EXIOBASE import by:

* Getting units from products
* Fixing unit consistency
* Removing some name quirks

### 0.7.11.1 (2019-10-29)

Change EXIOBASE importer to only include activities, not products

### 0.7.11 (2019-10-29)

* Add mapping file for SimaPro-ecoinvent 3.4. Thanks Pascal Lesage!
* Add importer for EXIOBASE 3.3 (IO, hybrid)

### 0.7.10 (2019-10-09)

Close [#61](https://bitbucket.org/cmutel/brightway2-io/issues/61/add-biosphere-flows-for-missing-cfs): Add missing biosphere flows when importing LCIA methods.

### 0.7.9 (2019-09-20)

Add CPC codes from single reference products during ecospold2 import.

### 0.7.8 (2019-09-19)

Merged [Pull Request #5](https://bitbucket.org/cmutel/brightway2-io/pull-requests/5/bug-fix-for-simapro-imports) to fix some SimaPro import issues. Thanks Benjamin Portner!

### 0.7.7 (2019-09-16)

Support ecoinvent 3.6

### 0.7.6 (2019-07-06)

* Exit Excel importer early if no data found
* Handle all columns cutoff in Excel importer
* Fix Gephi exporter for ecoinvent v3

### 0.7.5 (2019-06-17)

* Fix #59: Importing ecospold1 fails due to unset variable in extractor

### 0.7.4 (2019-02-25)

Fix location updating bug preventing clean import of ecoinvent 2.2

### 0.7.3 (2018-12-18)

Fix missing import bug

### 0.7.2 (2018-10-16)

* Catch multiprocessing errors from certain configurations

### 0.7.1 (2018-09-28)

* Add units to all ecoinvent 3.5 LCIA methods
* Fix up method name rationalization

## 0.7 (2018-09-10)

* Support ecoinvent 3.5
* Drop unused and outdated ecoinvent 31 biosphere flows
* Add CPC classification to ecoinvent imports
* Make importers play better with the Activity Browser
* Add tests for Ecospold2 extraction and importing
* Add `utils.standardize_method_to_len_3`
* Add optional strategy to rationalize default LCIA method names

## 0.6 (2018-05-31)

* Rewrite and test Excel importer and exporters to support parameters and data roundtrips
* Change `assign_only_product_as_production` to not overwrite existing fields
* Fix inconsistencies added in ecoinvent 3.4
* Update older location codes from ecoinvent

### 0.5.12 (2017-10-10)

* Add bugfix for numeric values in Excel importer

### 0.5.11 (2017-10-10)

* Support ecoinvent 3.4, including new biosphere flows. Use function `add_ecoinvent_34_biosphere_flows` to update old databases
* Update excel/CSV importers: Strip whitespace, and allow `**kwargs` in `write_database`
* Unit conversion update: don't always convert kilometers to meters, as some databases use both

### 0.5.10 (2017-06-16)

* Break uncertainty strategies for ecospold2 apart to allow for easier manipulation
* Allow CSV exporter to only export a selection of datasets
* Allow Damage methods to be imported from SimaPro LCIA csv (thanks James Joyce!)
* Add reference product to excel output

### 0.5.9 (2017-04-17)

* Fix license text

### 0.5.8 (2017-04-06)

* Fixes for Conda packaging and license encoding

### 0.5.7 (2017-01-12)

Improve imports for SimaPro CSV files:

* Improve flexibility of which fields to match against
* Add SimaPro conversions for ecoinvent 3.2 and 3.3
* Normalize and migrate SimaPro water flows
* Add SimaPro electricity conversion
* Handle allocated SimaPro production with zero production amounts

### 0.5.6 (2016-12-02)

A number of small changes to improve handling of SimaPro exports

### 0.5.5 (2016-11-10)

Updates for compatibility with ecoinvent 3.3 and 3.2.

### 0.5.4 (2016-09-27)

Updates for compatibility with ecoinvent 3.3 release.

### 0.5.3 (2016-07-14)

* Update for compatibility with bw2data 2.3.
* Add `overwrite` flags to default data creators.

### 0.5.2 (2016-07-01)

* Fixed bug in ecospold2 import which assumed standard deviation instead of variance (Thanks Guillaume Audard)
* Fixed bugs for CSV imports
* Prevent duplicate codes from being written, and don't overwrite existing codes

### 0.5.1 (2016-06-05)

* Updates for compatibility with bw2data 2.2.
* Make ``activity_hash`` less unforgiving

## 0.5 (2016-05-28)

* Don't raise nonunique error when linking unless linking to nonunique dataset descriptors
* Improve error messages when imported data has duplicate processes
* Properly close multiprocessing pools

### 0.4.1 (2016-04-15)

Bugfix release: Include `psutil`, an undeclared dependency of `pyprind`.

## 0.4 (2016-04-01)

0.4 release.
