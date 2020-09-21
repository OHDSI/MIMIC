# README #

###Refresh OMOP Vocabularies In A Single Script###

###Latest updates (recent first):###

*2020-09-11*

* Put it into MIMIC IV to OMOP project

###Usage:###

#####To refresh standard vocabularies#####

1. Download and unzip files from Athena (to a path known to vocabulary_refresh.py)

* set Athena local and gs path, target dataset and project name in vocabulary_refresh.conf
    * run `python vocabulary_refresh.py -s10`

* alternatively, run step by step:
    2. Copy files to GCP bucket
        * gsutil cp <athena files location>/\*.csv gs://the_project_bucket/vocabulary_YYYY_MM_DD/
    3. Load files to intermediate BQ tables
        * load_to_bq_vocab.py (uncomment content of voc list)
    4. Populate target vocabulary tables from intermediate tables
        * create_voc_from_tmp.sql

#####To refresh or add new custom mapping#####

* set custom mapping CSV local and gs path, target dataset and project name in vocabulary_refresh.conf
    * run `python vocabulary_refresh.py -s20`

* alternatively, run step by step:
    1. Copy custom mapping files to custom_mapping_csv/ folder, and update custom_mapping_list.tsv
    2. Copy custom mapping files to GCP bucket
        * gsutil cp custom_mapping_csv/\*.csv gs://some_path/CUSTOM_MAPPING/
    3. Load files to the intermediate BQ table (tmp_custom_mapping)
        * load_to_bq_vocab.py (uncomment value of custom_mapping_table variable)
        * check_custom_loaded.sql
    4. Add custom concepts to vocabulary tables from the intermediate table
        * custom_vocabularies.sql

#####Verify the result#####

* set target dataset and project name in vocabulary_refresh.conf
    * run `python vocabulary_refresh.py -s30`

* alternatively, run step by step:
    5. Verify target tables
        * vocabulary_check_bq.sql
    6. Clean up temporary tables from the previous step
        * vocabulary_cleanup_bq.sql

#####Comments#####

* custom_mapping_list.tsv is populated manually, and is helpful to track custom vocabularies and custom concepts ranges in use.

#####Tne End#####

