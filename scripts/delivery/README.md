# README #

###Export And Load OMOP Tables To And From BigQuery###

* The script `export_from_bq.py` exports BQ tables to the given Google Storage location, and optionally copies the created files to the given local location.
* Tables are exported into a single folder, to single files. If a table is too large, the script tries again and creates multiple files.
* To save files in the bucket only, select 'gs' export type, and to download locally, select 'local' export type.
* Copy parameters from `config_default` inside the script to a json .conf file to override default values.

###Usage:###

`python scripts/delivery/export_from_bq.py -c scripts/delivery/export_from_bq_demo.conf`

`python scripts/delivery/load_to_bq.py -c scripts/delivery/load_to_bq_demo.conf`

###Latest updates (recent first):###

*2021-02-07*

* Tables are exported into a single folder, to single files. If a table is too large, the script tries again and creates multiple files.

*2020-12-21*

* This Readme is created.

#####Tne End#####

