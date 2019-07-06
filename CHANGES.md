# Changelog

### 0.7.6 (2019-07-06)

* Exit Excel importer early if no data found
* Handle all columns cutoff in Excel importer
* Fix Gephi exporter for ecoinvent v3

### 0.7.5 (2019-06-17)

* Fix [#59: Importing ecospold1 fails due to unset variable in extractor
Create issue](https://bitbucket.org/cmutel/brightway2-io/issues/59/importing-ecospold1-fails-due-to-unset)

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
